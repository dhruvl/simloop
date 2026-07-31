"""Probe: an aiohttp web application served from a simulated host.

The request comes from a raw stream client on a second sim host rather than
from aiohttp's own client, so this measures exactly one thing: whether
aiohttp's server side speaks HTTP over the simulated network. The listener is
opened with ``loop.create_server`` because ``web.TCPSite`` cannot open it; the
site is probed separately by probe_aiohttp_site.
"""

from __future__ import annotations

import asyncio
from typing import Any

from simloop import SimLoop

LIBRARY = "aiohttp (server)"
DISTRIBUTION = "aiohttp"
TIER = 1
NOTES = "AppRunner + loop.create_server on a sim host; raw sim-stream client."

BODY = "hello from the simulation"
PORT = 8080
REQUEST = b"GET /hello HTTP/1.1\r\nHost: web\r\nConnection: close\r\n\r\n"


async def probe(loop: SimLoop) -> str:
    # Imported inside the probe so the module stays importable — and its
    # contract testable — without the probes dependency group installed.
    from aiohttp import web

    async def handle(request: Any) -> Any:
        return web.Response(text=BODY)

    app = web.Application()
    app.router.add_get("/hello", handle)
    runner = web.AppRunner(app)

    async def listen() -> Any:
        # Opening the listener inside a host task is what binds it to that
        # host; the driver task belongs to the implicit driver host.
        await runner.setup()
        return await asyncio.get_running_loop().create_server(
            runner.server, "0.0.0.0", PORT
        )

    async def fetch() -> bytes:
        reader, writer = await asyncio.open_connection("web", PORT)
        writer.write(REQUEST)
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        return response

    server = await loop.net.host("web").create_task(listen())
    try:
        response = await loop.net.host("client").create_task(fetch())
    finally:
        server.close()
        await runner.cleanup()

    status = response.split(b"\r\n", 1)[0].decode()
    body = response.rsplit(b"\r\n\r\n", 1)[-1].decode()
    return f"one GET over the sim network: {status!r}, body {body!r}"
