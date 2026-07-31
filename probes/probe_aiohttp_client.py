"""Probe: aiohttp's client session issuing one GET at a sim host.

A real listener answers on the target host and the host itself is registered,
so name resolution and the connection both have somewhere to go: whatever
stops this probe is aiohttp's connector, not the setup around it.
"""

from __future__ import annotations

import asyncio

from probes import _http
from simloop import SimLoop

LIBRARY = "aiohttp (client)"
DISTRIBUTION = "aiohttp"
TIER = 2
NOTES = "ClientSession GET at a sim host answered by a raw stream server."

PORT = 8080


async def probe(loop: SimLoop) -> str:
    import aiohttp

    async def listen() -> asyncio.AbstractServer:
        return await asyncio.start_server(_http.respond, "0.0.0.0", PORT)

    async def request() -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://web:{PORT}/hello") as response:
                text: str = await response.text()
                return text

    server = await loop.net.host("web").create_task(listen())
    try:
        body = await loop.net.host("client").create_task(request())
    finally:
        server.close()
        await server.wait_closed()
    return f"ClientSession GET returned {body!r}"
