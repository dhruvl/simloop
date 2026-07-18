"""Length-prefixed JSON framing and a one-shot request/response helper.

Every RPC in the demo opens a fresh connection, sends one JSON object,
reads one JSON object back, and closes. One connection per call keeps
request/response correlation trivial when a timeout abandons a call whose
reply is still somewhere in the network.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

_HEADER = struct.Struct(">I")
MAX_FRAME = 1 << 20


class FrameError(Exception):
    """The peer sent something that is not a framed JSON object."""


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    header = await reader.readexactly(_HEADER.size)
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME:
        raise FrameError(f"frame of {length} bytes exceeds MAX_FRAME")
    body = await reader.readexactly(length)
    try:
        message = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameError(f"undecodable frame: {exc}") from exc
    if not isinstance(message, dict):
        raise FrameError(f"expected a JSON object, got {type(message).__name__}")
    return message


def write_message(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
    body = json.dumps(message, separators=(",", ":")).encode("utf-8")
    writer.write(_HEADER.pack(len(body)) + body)


async def call(
    host: str, port: int, message: dict[str, Any], *, timeout_s: float
) -> dict[str, Any] | None:
    """One request/response on a fresh connection; ``None`` on any failure.

    Timeouts, refused connections, and torn streams all collapse to ``None``
    because the caller's only recourse is the same either way: back off and
    retry. Under simulation a partition stalls silently and a crashed peer
    sends no reset, so the timeout is the only failure detector there is.
    """
    try:
        async with asyncio.timeout(timeout_s):
            reader, writer = await asyncio.open_connection(host, port)
            try:
                write_message(writer, message)
                await writer.drain()
                return await read_message(reader)
            finally:
                writer.close()
    except (TimeoutError, OSError, asyncio.IncompleteReadError, FrameError):
        return None
