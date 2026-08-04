"""Probe: a websockets server and client talking over wss:// in simulation.

Both ends are the library's own asyncio implementation, on separate sim
hosts, so this is the one probe that drives ``create_server(ssl=...)`` and
``create_connection(ssl=...)`` in a single run.
"""

from __future__ import annotations

from typing import Any

from probes import _tls
from simloop import SimLoop

LIBRARY = "websockets (wss)"
DISTRIBUTION = "websockets"
TIER = 1
NOTES = "asyncio server and client on two sim hosts, over wss://."

PORT = 8443


async def probe(loop: SimLoop) -> str:
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    certificate = _tls.server_context("ws")
    trust = _tls.client_context()

    async def echo(connection: Any) -> None:
        async for message in connection:
            await connection.send(message.upper())

    async def listen() -> Any:
        # Awaiting the server object is what starts it; doing that inside a
        # host task is what binds the listener to that host.
        return await serve(echo, "0.0.0.0", PORT, ssl=certificate)

    async def talk() -> str:
        async with connect(f"wss://ws:{PORT}/", ssl=trust) as client:
            await client.send("hello")
            reply: str = await client.recv()
            return reply

    server = await loop.net.host("ws").create_task(listen())
    try:
        reply = await loop.net.host("client").create_task(talk())
    finally:
        server.close()
        await server.wait_closed()
    return f"handshake, one echoed frame ({reply!r}) and close over wss://"
