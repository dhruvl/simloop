"""Reference TLS workload for replay-stability checks.

Importable for in-process runs; also runnable as a script —
``python tests/replay_tls_workload.py <seed>`` prints one line:
``<trace_hash> <result_digest>``. One TLS server, three TLS clients on
machines of their own, a jittered link, a partition that heals and a host
crash, so every fault draw is part of the replay proof. The certificate is
minted fresh on every run, which is what makes comparing runs across
processes mean anything.
"""

from __future__ import annotations

import asyncio
import hashlib
import ssl
import sys
from typing import Any

from _tls_certs import client_context, server_context
from simloop import Host, SimLoop, sim

_PORT = 8443


class _Echo(asyncio.Protocol):
    """Answers one message in upper case and hangs up."""

    def __init__(self) -> None:
        self._transport: Any = None

    def connection_made(self, transport: Any) -> None:
        self._transport = transport

    def data_received(self, data: bytes) -> None:
        self._transport.write(data.upper())
        self._transport.close()


class _Ask(asyncio.Protocol):
    """Sends one message and waits for the answer.

    A connection lost before an answer resolves the future with a marker
    rather than an exception: nothing here retrieves an exception a partition
    or a crash caused, and an unretrieved one would fail the whole run.
    """

    def __init__(self, message: bytes) -> None:
        self._message = message
        self.reply: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: Any) -> None:
        transport.write(self._message)

    def data_received(self, data: bytes) -> None:
        if not self.reply.done():
            self.reply.set_result(data)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.reply.done():
            self.reply.set_result(b"<lost>")


async def _ask(name: str, context: ssl.SSLContext) -> str:
    running: Any = asyncio.get_running_loop()
    message = f"{name}-{sim.uuid4()}".encode()
    try:
        async with asyncio.timeout(3.0):
            transport, protocol = await running.create_connection(
                lambda: _Ask(message),
                "api",
                _PORT,
                ssl=context,
                server_hostname="api",
            )
            reply: bytes = await protocol.reply
        transport.close()
        return reply.decode()
    except (TimeoutError, OSError) as exc:
        return type(exc).__name__


async def _chatter(context: ssl.SSLContext) -> None:
    running: Any = asyncio.get_running_loop()
    while True:
        transport, protocol = await running.create_connection(
            lambda: _Ask(b"noise"),
            "api",
            _PORT,
            ssl=context,
            server_hostname="api",
        )
        await protocol.reply
        transport.close()
        await asyncio.sleep(0.05)


async def _main(loop: SimLoop, hosts: dict[str, Host]) -> str:
    net = loop.net
    certificate = server_context("api")

    async def serve() -> Any:
        running: Any = asyncio.get_running_loop()
        return await running.create_server(_Echo, "0.0.0.0", _PORT, ssl=certificate)

    server = await hosts["api"].create_task(serve())
    hosts["noisy"].create_task(_chatter(client_context()))
    tasks = [
        hosts[name].create_task(_ask(name, client_context()))
        for name in ("one", "two", "three")
    ]

    loop.call_later(0.05, net.partition, {"two"}, {"api"})
    loop.call_later(0.40, net.heal)
    loop.call_later(0.10, net.crash, "noisy")

    results = [await task for task in tasks]
    await asyncio.sleep(2.0)
    server.close()
    await server.wait_closed()
    return repr(results)


def run(seed: int) -> str:
    loop = SimLoop(seed)
    net = loop.net
    hosts = {
        name: net.host(name) for name in ("api", "one", "two", "three", "noisy")
    }
    net.set_defaults(latency=(0.001, 0.02))
    net.set_link("three", "api", latency=(0.005, 0.05))
    try:
        outcome = loop.run_until_complete(_main(loop, hosts))
    finally:
        loop.close()
    digest = hashlib.sha256(outcome.encode()).hexdigest()
    return f"{loop.trace_hash()} {digest}"


if __name__ == "__main__":
    print(run(int(sys.argv[1])))
