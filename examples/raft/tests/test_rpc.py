"""The framed one-shot RPC helper, exercised under the simulated network."""

from __future__ import annotations

import asyncio

from simloop import SimLoop, sim_test

from raft import wire


def sim_loop() -> SimLoop:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    return loop


async def _echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    message = await wire.read_message(reader)
    wire.write_message(writer, {"echo": message["value"]})
    await writer.drain()
    writer.close()


async def _serve() -> None:
    server = await asyncio.start_server(_echo, "0.0.0.0", 4400)
    async with server:
        await server.serve_forever()


@sim_test
async def test_call_round_trips_one_message() -> None:
    loop = sim_loop()
    loop.net.host("server").create_task(_serve())
    await asyncio.sleep(0.05)
    reply = await wire.call("server", 4400, {"value": 7}, timeout_s=1.0)
    assert reply == {"echo": 7}


@sim_test
async def test_call_collapses_refusal_to_none() -> None:
    loop = sim_loop()
    loop.net.host("server")  # nothing listening: the connect is refused
    reply = await wire.call("server", 4400, {"value": 7}, timeout_s=0.5)
    assert reply is None


@sim_test
async def test_call_collapses_silence_to_none() -> None:
    loop = sim_loop()
    loop.net.host("server").create_task(_serve())
    client = loop.net.host("client")
    await asyncio.sleep(0.05)
    loop.net.partition(["server"], ["client"])
    reply = await client.create_task(
        wire.call("server", 4400, {"value": 7}, timeout_s=0.5)
    )
    assert reply is None
