"""Probe: aiohttp's documented way to start a server, ``web.TCPSite``.

``AppRunner`` plus ``TCPSite`` is what aiohttp's own documentation and
``web.run_app`` use, so it is worth its own row: whatever the request path can
do, this is the call an unmodified application makes to start listening.
"""

from __future__ import annotations

from typing import Any

from simloop import SimLoop

LIBRARY = "aiohttp (web.TCPSite)"
DISTRIBUTION = "aiohttp"
TIER = 1
NOTES = "The documented AppRunner + TCPSite startup path, nothing else."

PORT = 8081


async def probe(loop: SimLoop) -> str:
    from aiohttp import web

    async def handle(request: Any) -> Any:
        return web.Response(text="hello")

    app = web.Application()
    app.router.add_get("/hello", handle)
    runner = web.AppRunner(app)

    async def serve() -> None:
        await runner.setup()
        try:
            await web.TCPSite(runner, "0.0.0.0", PORT).start()
        finally:
            await runner.cleanup()

    await loop.net.host("web").create_task(serve())
    return f"web.TCPSite(...).start() bound port {PORT} on a sim host"
