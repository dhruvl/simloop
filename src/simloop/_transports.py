"""Transport implementations over the simulated packet network.

These are genuine ``asyncio`` transports driving user-supplied protocols, so
code written against the standard transport/protocol contract — including
``asyncio.open_connection`` and ``start_server`` — runs unchanged. All actual
packet movement is delegated to the owning ``SimNetwork``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from simloop._net import SimNetwork

_Addr = tuple[str, int]


def _check_bytes(data: object) -> bytes:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(
            f"data argument must be a bytes-like object, not {type(data).__name__!r}"
        )
    return bytes(data)


class _SimDatagramTransport(asyncio.DatagramTransport):
    def __init__(self, net: SimNetwork, local: _Addr, remote: _Addr | None) -> None:
        super().__init__()
        self._net = net
        self._local = local
        self._remote = remote
        self._protocol: Any = None
        self._closing = False

    def _begin(self, protocol: Any) -> None:
        self._protocol = protocol
        protocol.connection_made(self)

    def _datagram_arrived(self, data: bytes, addr: _Addr) -> None:
        if self._closing:
            return
        # A connected endpoint only hears its configured peer host and port.
        if self._remote is not None and addr != self._remote:
            return
        self._protocol.datagram_received(data, addr)

    def sendto(self, data: Any, addr: Any = None) -> None:
        payload = _check_bytes(data)
        if self._closing:
            raise RuntimeError("Cannot send on closing transport")
        target = addr if addr is not None else self._remote
        if target is None:
            raise ValueError("no address is set")
        if self._remote is not None and tuple(target) != self._remote:
            raise ValueError(f"Invalid address: must be None or {self._remote}")
        self._net._send_datagram(self._local, (target[0], target[1]), payload)

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._net._unbind_datagram(self._local)
        self._net._loop.call_soon(self._lost)

    def abort(self) -> None:
        self.close()

    def _lost(self) -> None:
        protocol, self._protocol = self._protocol, None
        if protocol is not None:
            protocol.connection_lost(None)

    def is_closing(self) -> bool:
        return self._closing

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == "sockname":
            return self._local
        if name == "peername":
            return self._remote
        return default

    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol
