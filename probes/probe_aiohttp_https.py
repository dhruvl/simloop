"""Probe: aiohttp's client session issuing one GET over https at a sim host.

The connector reaches the network the same way it does over plain http — a
real descriptor through sock_connect, then create_connection — but this time
with ``ssl`` and ``server_hostname`` riding alongside the socket, and it reads
the TLS layer back out of the transport afterwards.
"""

from __future__ import annotations

import asyncio

from probes import _http, _tls
from simloop import SimLoop

LIBRARY = "aiohttp (client, https)"
DISTRIBUTION = "aiohttp"
TIER = 2
NOTES = "ClientSession GET over https at a sim host with a minted certificate."

PORT = 8443


async def probe(loop: SimLoop) -> str:
    import aiohttp

    certificate = _tls.server_context("web")
    trust = _tls.client_context()

    async def listen() -> asyncio.AbstractServer:
        return await asyncio.start_server(
            _http.respond, "0.0.0.0", PORT, ssl=certificate
        )

    async def request() -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://web:{PORT}/hello", ssl=trust
            ) as response:
                text: str = await response.text()
                return text

    server = await loop.net.host("web").create_task(listen())
    try:
        body = await loop.net.host("client").create_task(request())
    finally:
        # A TLS close-notify exchange costs a round trip, so the responder
        # needs a moment to finish before the run ends.
        await asyncio.sleep(1.0)
        server.close()
        await server.wait_closed()
    return f"ClientSession GET over https returned {body!r}"
