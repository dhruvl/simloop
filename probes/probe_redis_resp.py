"""Probe: the Redis wire protocol (RESP) over simulated streams.

No Redis client library is involved: every real one needs a live server to
reach even its first command, which a simulation cannot provide. What is
testable without one is the pattern those clients are built from — a
request/response command protocol with length-prefixed framing over a single
long-lived connection — so this probe encodes and decodes RESP by hand
against a minimal server on another sim host.
"""

from __future__ import annotations

import asyncio

from simloop import SimLoop

LIBRARY = "redis (RESP wire protocol)"
DISTRIBUTION = None
TIER = 1
NOTES = "Hand-rolled RESP over sim streams; no client library, no real server."

PORT = 6379


def _command(*parts: str) -> bytes:
    encoded = b"".join(
        f"${len(part)}\r\n{part}\r\n".encode() for part in parts
    )
    return f"*{len(parts)}\r\n".encode() + encoded


async def _serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Answer PING, ECHO and GET/SET from an in-memory dict."""
    store: dict[str, str] = {}
    while header := await reader.readline():
        argv: list[str] = []
        for _ in range(int(header[1:])):
            length = int((await reader.readline())[1:])
            argv.append((await reader.readexactly(length + 2))[:length].decode())
        name = argv[0].upper()
        if name == "PING":
            writer.write(b"+PONG\r\n")
        elif name == "SET":
            store[argv[1]] = argv[2]
            writer.write(b"+OK\r\n")
        elif name == "GET":
            value = store.get(argv[1])
            writer.write(
                b"$-1\r\n" if value is None else f"${len(value)}\r\n{value}\r\n".encode()
            )
        else:
            writer.write(f"-ERR unknown command '{name}'\r\n".encode())
        await writer.drain()
    writer.close()
    await writer.wait_closed()


async def probe(loop: SimLoop) -> str:
    async def listen() -> asyncio.AbstractServer:
        return await asyncio.start_server(_serve, "0.0.0.0", PORT)

    async def client() -> list[str]:
        reader, writer = await asyncio.open_connection("redis", PORT)
        replies: list[str] = []
        for command in (
            _command("PING"),
            _command("SET", "seed", "0"),
            _command("GET", "seed"),
        ):
            writer.write(command)
            await writer.drain()
            head = (await reader.readline()).decode().rstrip("\r\n")
            if head.startswith("$"):
                head = (await reader.readexactly(int(head[1:]) + 2)).decode().rstrip()
            replies.append(head)
        writer.close()
        await writer.wait_closed()
        return replies

    server = await loop.net.host("redis").create_task(listen())
    try:
        replies = await loop.net.host("client").create_task(client())
    finally:
        # Let the handler see the client's EOF and finish on its own, so the
        # run ends with nothing pending.
        await asyncio.sleep(0.1)
        server.close()
        await server.wait_closed()
    return f"PING, SET and GET round trips over one connection: {replies}"
