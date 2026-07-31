"""A minimal HTTP/1.1 responder for the client probes to aim at.

The client probes exist to find out how far a client library gets, so they
must have something real to reach: a probe pointed at a dead port could only
ever report a refusal. This is hand-written rather than aiohttp's server so
that a client probe's verdict describes the client alone.
"""

from __future__ import annotations

import asyncio

BODY = b"hello from the simulation"

RESPONSE = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"Content-Length: " + str(len(BODY)).encode() + b"\r\n"
    b"Connection: close\r\n"
    b"\r\n" + BODY
)


async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Answer one request with a fixed 200, then close the connection."""
    await reader.readuntil(b"\r\n\r\n")
    writer.write(RESPONSE)
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()
