"""Reference multi-host network workload for replay-stability checks.

Importable for in-process runs; also runnable as a script —
``python tests/replay_net_workload.py <seed>`` prints one line:
``<trace_hash> <result_digest>``. It exercises streams, datagrams, link
faults, a partition that heals, and a host crash, so every fault-injection
draw is part of the replay proof.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from typing import Any

from simloop import Host, SimLoop, sim

_PINGS = 12


class _PingCounter(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.log: list[str] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.log.append(f"{data.decode()}@{addr[0]}")


async def _echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        async with asyncio.timeout(3.0):
            while line := await reader.readline():
                await asyncio.sleep(sim.random.uniform(0.001, 0.01))
                writer.write(line.upper())
                await writer.drain()
    except TimeoutError:
        pass
    finally:
        writer.close()


async def _serve() -> None:
    server = await asyncio.start_server(_echo, "0.0.0.0", 9000)
    async with server:
        await asyncio.sleep(6.0)


async def _client(name: str, lines: int) -> list[str]:
    reader, writer = await asyncio.open_connection("hub", 9000)
    replies: list[str] = []
    for number in range(lines):
        writer.write(f"{name}-{number}-{sim.uuid4()}\n".encode())
        await writer.drain()
        replies.append((await reader.readline()).decode())
        await asyncio.sleep(sim.random.uniform(0.001, 0.02))
    writer.close()
    await writer.wait_closed()
    return replies


async def _chatter() -> None:
    reader, writer = await asyncio.open_connection("hub", 9000)
    try:
        while True:
            writer.write(b"noise\n")
            await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        raise


async def _ping(transport: asyncio.DatagramTransport) -> None:
    for number in range(_PINGS):
        transport.sendto(f"ping{number}".encode(), ("alpha", 7000))
        await asyncio.sleep(0.02)


async def _main(loop: SimLoop, hosts: dict[str, Host]) -> str:
    net = loop.net
    hub, alpha, beta, gamma = (hosts[n] for n in ("hub", "alpha", "beta", "gamma"))

    serve_task = hub.create_task(_serve())
    await asyncio.sleep(0.01)

    counter = _PingCounter()

    async def bind_counter() -> asyncio.DatagramTransport:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: counter, local_addr=("0.0.0.0", 7000)
        )
        return transport

    async def bind_pinger() -> asyncio.DatagramTransport:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("0.0.0.0", 7001)
        )
        return transport

    counter_transport = await alpha.create_task(bind_counter())
    pinger_transport = await beta.create_task(bind_pinger())

    tasks = [
        alpha.create_task(_client("alpha", 4)),
        beta.create_task(_client("beta", 4)),
        beta.create_task(_ping(pinger_transport)),
    ]
    gamma.create_task(_chatter())

    loop.call_later(0.08, net.partition, {"beta"}, {"hub", "alpha"})
    loop.call_later(0.20, net.heal)
    loop.call_later(0.12, net.crash, "gamma")

    results: list[Any] = []
    for task in tasks:
        results.append(await task)
    await asyncio.sleep(4.0)  # let echo handlers time out and finish cleanly
    serve_task.cancel()
    try:
        await serve_task
    except asyncio.CancelledError:
        pass
    counter_transport.close()
    pinger_transport.close()
    await asyncio.sleep(0.01)
    results.append(sorted(counter.log))
    return repr(results)


def run(seed: int) -> str:
    loop = SimLoop(seed)
    net = loop.net
    hosts = {name: net.host(name) for name in ("hub", "alpha", "beta", "gamma")}
    net.set_defaults(latency=(0.001, 0.03))
    net.set_link("beta", "alpha", drop=0.25, duplicate=0.25)
    try:
        outcome = loop.run_until_complete(_main(loop, hosts))
    finally:
        loop.close()
    digest = hashlib.sha256(outcome.encode()).hexdigest()
    return f"{loop.trace_hash()} {digest}"


if __name__ == "__main__":
    print(run(int(sys.argv[1])))
