"""Probe: a websockets server and client talking to each other in simulation.

Both ends are the library's own asyncio implementation, on separate sim hosts,
so the handshake and every frame cross the simulated network. No TLS: it is
fenced, and this probe deliberately stays away from it.
"""

from __future__ import annotations

from typing import Any

from simloop import SimLoop

LIBRARY = "websockets"
DISTRIBUTION = "websockets"
TIER = 1
NOTES = "asyncio server and client on two sim hosts, ws:// only."

PORT = 8765


async def probe(loop: SimLoop) -> str:
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    async def echo(connection: Any) -> None:
        async for message in connection:
            await connection.send(message.upper())

    async def listen() -> Any:
        # Awaiting the server object is what starts it; doing that inside a
        # host task is what binds the listener to that host.
        return await serve(echo, "0.0.0.0", PORT)

    async def talk() -> str:
        async with connect(f"ws://ws:{PORT}/") as client:
            await client.send("hello")
            reply: str = await client.recv()
            return reply

    server = await loop.net.host("ws").create_task(listen())
    try:
        reply = await loop.net.host("client").create_task(talk())
    finally:
        server.close()
        await server.wait_closed()
    return f"handshake, one echoed frame ({reply!r}) and close over ws://"
