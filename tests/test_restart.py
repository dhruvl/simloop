"""A crashed host coming back: fresh incarnation, same address, dead past."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from simloop import SimLoop


def _network(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    loop.net.set_defaults(latency=(0.001, 0.001))
    return loop


async def _echo_once(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(await reader.readline())
    writer.close()


async def _ask(text: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("server", 9000)
    writer.write(text)
    reply = await reader.readline()
    writer.close()
    return reply


def test_restarted_host_serves_again_on_the_same_port() -> None:
    loop = _network()

    async def main() -> tuple[bytes, bytes]:
        async def boot() -> None:
            await asyncio.start_server(_echo_once, "0.0.0.0", 9000)

        await loop.net.host("server").create_task(boot())
        await asyncio.sleep(0.01)
        first = await loop.net.host("client").create_task(_ask(b"one\n"))
        loop.net.crash("server")
        loop.net.restart("server")
        await loop.net.host("server").create_task(boot())
        await asyncio.sleep(0.01)
        second = await loop.net.host("client").create_task(_ask(b"two\n"))
        return first, second

    try:
        first, second = loop.run_until_complete(main())
    finally:
        loop.close()
    assert (first, second) == (b"one\n", b"two\n")


def test_restart_requires_a_crash_first() -> None:
    loop = _network()
    with pytest.raises(ValueError, match="not crashed"):
        loop.net.restart("server")
    loop.close()


def test_restart_requires_a_known_host() -> None:
    loop = _network()
    with pytest.raises(OSError, match="ghost"):
        loop.net.restart("ghost")
    loop.close()


def test_dead_window_traffic_is_lost_but_new_traffic_flows() -> None:
    loop = _network()
    heard: list[bytes] = []

    class Listener(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: Any) -> None:
            heard.append(data)

    async def main() -> None:
        async def bind() -> None:
            await loop.create_datagram_endpoint(Listener, local_addr=("0.0.0.0", 5000))

        async def send(payload: bytes) -> None:
            transport, _ = await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol, remote_addr=("server", 5000)
            )
            transport.sendto(payload)
            transport.close()

        await loop.net.host("server").create_task(bind())
        loop.net.crash("server")
        await loop.net.host("client").create_task(send(b"into the void"))
        await asyncio.sleep(0.01)  # delivery attempt lands while dead
        loop.net.restart("server")
        await loop.net.host("server").create_task(bind())
        await loop.net.host("client").create_task(send(b"welcome back"))
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert heard == [b"welcome back"]


def test_crash_and_restart_appear_in_the_trace() -> None:
    loop = _network()

    async def main() -> None:
        loop.net.crash("server")
        loop.net.restart("server")

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    events = [
        event
        for event in loop.trace
        if "crash server" in event.label or "restart server" in event.label
    ]
    assert len(events) == 2


def test_same_seed_replays_a_restart_schedule_identically() -> None:
    def run() -> str:
        loop = _network(seed=7)

        async def main() -> None:
            async def boot() -> None:
                await asyncio.start_server(_echo_once, "0.0.0.0", 9000)

            await loop.net.host("server").create_task(boot())
            await asyncio.sleep(0.01)
            await loop.net.host("client").create_task(_ask(b"a\n"))
            loop.net.crash("server")
            loop.net.restart("server")
            await loop.net.host("server").create_task(boot())
            await asyncio.sleep(0.01)
            await loop.net.host("client").create_task(_ask(b"b\n"))

        try:
            loop.run_until_complete(main())
            return loop.trace_hash()
        finally:
            loop.close()

    assert run() == run()
