"""Probe: httpx's async client issuing one GET at a sim host.

httpx reaches the network through httpcore and anyio, so this probe says
where that stack — not httpx's own code — leaves the simulation. A real
listener answers on the target host, so a verdict short of a response is the
client's doing and not a missing server.
"""

from __future__ import annotations

import asyncio

from probes import _http
from simloop import SimLoop

LIBRARY = "httpx"
DISTRIBUTION = "httpx"
TIER = 2
NOTES = "AsyncClient GET at a sim host answered by a raw stream server."

PORT = 8080


async def probe(loop: SimLoop) -> str:
    import httpx

    async def listen() -> asyncio.AbstractServer:
        return await asyncio.start_server(_http.respond, "0.0.0.0", PORT)

    async def request() -> str:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://web:{PORT}/hello")
            text: str = response.text
            return text

    server = await loop.net.host("web").create_task(listen())
    try:
        body = await loop.net.host("client").create_task(request())
    finally:
        server.close()
        await server.wait_closed()
    return f"AsyncClient GET returned {body!r}"
