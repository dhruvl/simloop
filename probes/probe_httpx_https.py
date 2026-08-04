"""Probe: httpx's async client issuing one GET over https at a sim host.

httpx reaches no loop TLS API at all — httpcore wraps the byte stream with
anyio's memory-BIO TLSStream, in process — so this row says something the
aiohttp one cannot: that a library running its own TLS engine now has a
simulated peer to speak TLS to.
"""

from __future__ import annotations

import asyncio

from probes import _http, _tls
from simloop import SimLoop

LIBRARY = "httpx (https)"
DISTRIBUTION = "httpx"
TIER = 2
NOTES = "AsyncClient GET over https; the TLS engine is anyio's, not the loop's."

PORT = 8443


async def probe(loop: SimLoop) -> str:
    import httpx

    certificate = _tls.server_context("web")
    trust = _tls.client_context()

    async def listen() -> asyncio.AbstractServer:
        return await asyncio.start_server(
            _http.respond, "0.0.0.0", PORT, ssl=certificate
        )

    async def request() -> str:
        async with httpx.AsyncClient(verify=trust) as client:
            response = await client.get(f"https://web:{PORT}/hello")
            text: str = response.text
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
    return f"AsyncClient GET over https returned {body!r}"
