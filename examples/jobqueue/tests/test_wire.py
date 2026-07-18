"""Framing round-trips, frame limits, and the one-shot call helper."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from simloop import SimLoop, sim_test

from jobqueue import wire

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _run(coro: Any) -> Any:
    loop = SimLoop(seed=0)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_oversized_frame_is_rejected() -> None:
    async def main() -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(wire._HEADER.pack(wire.MAX_FRAME + 1))
        with pytest.raises(wire.FrameError):
            await wire.read_message(reader)

    _run(main())


def test_non_object_payload_is_rejected() -> None:
    async def main() -> None:
        reader = asyncio.StreamReader()
        body = b"[1, 2]"
        reader.feed_data(wire._HEADER.pack(len(body)) + body)
        reader.feed_eof()
        with pytest.raises(wire.FrameError):
            await wire.read_message(reader)

    _run(main())


@sim_test(seeds=5)
async def test_call_round_trips_a_message() -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    loop.net.host("server")
    loop.net.host("client")

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        message = await wire.read_message(reader)
        wire.write_message(writer, {"echo": message})
        await writer.drain()
        writer.close()

    async def serve() -> None:
        server = await asyncio.start_server(handle, "0.0.0.0", 7000)
        async with server:
            await server.serve_forever()

    loop.net.host("server").create_task(serve(), name="server")
    await asyncio.sleep(0.05)
    reply = await loop.net.host("client").create_task(
        wire.call("server", 7000, {"op": "ping"}, timeout_s=1.0)
    )
    assert reply == {"echo": {"op": "ping"}}


@sim_test(seeds=5)
async def test_call_returns_none_when_nobody_listens() -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    loop.net.host("a")
    loop.net.host("b")
    reply = await loop.net.host("a").create_task(
        wire.call("b", 7000, {"op": "ping"}, timeout_s=0.5)
    )
    assert reply is None


def test_jobqueue_stays_stdlib_only() -> None:
    code = (
        "import sys\n"
        "import jobqueue.wire\n"
        "import jobqueue.store\n"
        "import jobqueue.broker\n"
        "import jobqueue.client\n"
        "import jobqueue.worker\n"
        "bad = sorted(m for m in sys.modules"
        " if m.split('.')[0] in ('simloop', 'pytest'))\n"
        "assert not bad, bad\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PACKAGE_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
