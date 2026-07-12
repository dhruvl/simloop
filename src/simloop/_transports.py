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


class _SimStreamTransport(asyncio.Transport):
    """One end of a reliable, ordered byte-stream connection.

    Reliability comes from per-direction sequence numbers dispatched in
    order by the network, not from retransmission: stream packets are never
    dropped, only delayed or held. Flow control is not simulated — writes
    leave immediately, so the reported write-buffer size is always zero and
    the peer can never pause this side.
    """

    def __init__(
        self, net: SimNetwork, conn: int, local: _Addr, remote: _Addr
    ) -> None:
        super().__init__()
        self._net = net
        self._conn = conn
        self._local = local
        self._remote = remote
        self._protocol: Any = None
        self._out_seq = 1  # seq 0 was this direction's handshake packet
        self._closing = False
        self._closed = False
        self._eof_sent = False
        self._read_paused = False
        self._backlog: list[bytes] = []
        self._eof_pending = False
        self._limits = (16 * 1024, 64 * 1024)  # (low, high): recorded, inert

    def _begin(self, protocol: Any) -> None:
        self._protocol = protocol
        protocol.connection_made(self)

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    def _send(self, kind: str, payload: bytes = b"") -> None:
        seq = self._out_seq
        self._out_seq += 1
        self._net._send_stream(
            kind=kind,
            src=self._local[0],
            dst=self._remote[0],
            conn=self._conn,
            seq=seq,
            payload=payload,
        )

    def write(self, data: Any) -> None:
        payload = _check_bytes(data)
        if self._eof_sent or self._closing or self._closed:
            raise RuntimeError("Cannot write to closing transport")
        if payload:
            self._send("data", payload)

    def writelines(self, list_of_data: Any) -> None:
        for data in list_of_data:
            self.write(data)

    def write_eof(self) -> None:
        if self._eof_sent or self._closed:
            return
        self._eof_sent = True
        self._send("fin")

    def can_write_eof(self) -> bool:
        return True

    def close(self) -> None:
        if self._closing or self._closed:
            return
        self._closing = True
        if not self._eof_sent:
            self._eof_sent = True
            self._send("fin")
        self._net._loop.call_soon(self._finish, None)

    def abort(self) -> None:
        if self._closed:
            return
        self._closing = True
        self._send("rst")
        self._net._loop.call_soon(self._finish, None)

    def _finish(self, exc: Exception | None) -> None:
        if self._closed:
            return
        self._closed = True
        self._closing = True
        self._net._drop_stream(self._conn, self._local[0])
        protocol, self._protocol = self._protocol, None
        if protocol is not None:
            protocol.connection_lost(exc)

    # ------------------------------------------------------------------
    # Inbound (called by the network, already in seq order)
    # ------------------------------------------------------------------

    def _data_arrived(self, data: bytes) -> None:
        if self._closed:
            return
        if self._read_paused:
            self._backlog.append(data)
            return
        self._protocol.data_received(data)

    def _eof_arrived(self) -> None:
        if self._closed:
            return
        if self._read_paused:
            self._eof_pending = True
            return
        keep_open = self._protocol.eof_received()
        if not keep_open:
            self.close()

    def _reset_arrived(self) -> None:
        if self._closed:
            return
        self._finish(ConnectionResetError("Connection reset by peer"))

    # ------------------------------------------------------------------
    # Read flow control (honored locally; never propagates to the peer)
    # ------------------------------------------------------------------

    def pause_reading(self) -> None:
        if self._closed or self._read_paused:
            return
        self._read_paused = True

    def resume_reading(self) -> None:
        if self._closed or not self._read_paused:
            return
        self._read_paused = False
        while self._backlog and not self._read_paused and not self._closed:
            self._protocol.data_received(self._backlog.pop(0))
        if self._eof_pending and not self._read_paused and not self._closed:
            self._eof_pending = False
            self._eof_arrived()

    def is_reading(self) -> bool:
        return not self._read_paused and not self._closed

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def is_closing(self) -> bool:
        return self._closing or self._closed

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

    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        if high is None:
            high = 64 * 1024 if low is None else 4 * low
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(
                f"high ({high!r}) must be >= low ({low!r}) must be >= 0"
            )
        self._limits = (low, high)

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return self._limits

    def get_write_buffer_size(self) -> int:
        return 0
