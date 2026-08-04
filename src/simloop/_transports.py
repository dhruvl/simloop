"""Transport implementations over the simulated packet network.

These are genuine ``asyncio`` transports driving user-supplied protocols, so
code written against the standard transport/protocol contract — including
``asyncio.open_connection`` and ``start_server`` — runs unchanged. All actual
packet movement is delegated to the owning ``SimNetwork``.
"""

from __future__ import annotations

import asyncio
import socket
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


def _check_limits(high: int | None, low: int | None) -> tuple[int, int]:
    """Derive a (low, high) watermark pair the way the stdlib derives it.

    The rules and the error text match
    ``asyncio.transports._FlowControlMixin._set_write_buffer_limits``, so a
    protocol that already knows what its own transport accepts learns nothing
    new here. Shared with the network default so both are checked once.
    """
    if high is None:
        high = 64 * 1024 if low is None else 4 * low
    if low is None:
        low = high // 4
    if not high >= low >= 0:
        raise ValueError(f"high ({high!r}) must be >= low ({low!r}) must be >= 0")
    return (low, high)


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


class _SimSocket:
    """Stand-in for the OS socket a simulated stream connection does not have.

    Client stacks introspect the transport's socket: anyio eagerly calls
    getpeername()/.family on every typed-attribute lookup, aiohttp sets
    TCP_NODELAY through it, and httpcore polls fileno() to decide whether a
    pooled connection died. Addresses are reported as synthetic-IP tuples so
    callers that feed them to ipaddress.ip_address() get a plausible
    AF_INET sockaddr; option and teardown calls are accepted and do nothing.
    Anything not listed here raises AttributeError, keeping the simulation
    loud about surface it does not fake.
    """

    def __init__(self, transport: _SimStreamTransport) -> None:
        self._transport = transport
        self.family = socket.AF_INET
        self.type = socket.SOCK_STREAM
        self.proto = socket.IPPROTO_TCP
        self._park: tuple[socket.socket, socket.socket] | None = None
        self._disposed = False

    def _address(self, endpoint: _Addr) -> tuple[str, int]:
        name, port = endpoint
        return (self._transport._net.address(name), port)

    def getsockname(self) -> tuple[str, int]:
        return self._address(self._transport._local)

    def getpeername(self) -> tuple[str, int]:
        return self._address(self._transport._remote)

    def setsockopt(self, *args: Any) -> None:
        return None

    def shutdown(self, how: int) -> None:
        return None

    def close(self) -> None:
        return None

    def fileno(self) -> int:
        # httpcore's is_socket_readable() polls this fd to decide whether a
        # pooled connection died. A parked socketpair end reads not-readable
        # until its peer end is closed, so the poll answers exactly one
        # question: has the peer's EOF arrived? A reset or a teardown closes
        # both ends instead and reports -1, which the same poll reads as dead
        # just as a closed real socket would. Lazy: connections nobody
        # introspects cost no descriptors, which matters at campaign scale.
        if self._disposed:
            return -1
        if self._park is None:
            self._park = socket.socketpair()
            # The peer's FIN/RST can land before anyone asks for the socket,
            # so a freshly parked pair inherits whatever the transport
            # already saw rather than claiming a dead connection is live.
            if self._transport._peer_closed:
                self._park[0].close()
        return self._park[1].fileno()

    def _peer_gone(self) -> None:
        # The peer's EOF arrived: closing the held end makes the exposed end
        # poll readable, exactly when a real kernel would report it.
        if self._park is not None:
            self._park[0].close()

    def _dispose(self) -> None:
        self._disposed = True
        if self._park is not None:
            self._park[0].close()
            self._park[1].close()
            self._park = None


class _SimStreamTransport(asyncio.Transport):
    """One end of a reliable, ordered byte-stream connection.

    Reliability comes from per-direction sequence numbers dispatched in
    order by the network, not from retransmission: stream packets are never
    dropped, only delayed or held.

    The write buffer holds every byte written that the peer's protocol has
    not received yet — in flight, held by a partition, waiting on an earlier
    sequence number, or parked because the peer paused reading. Bytes are
    charged in ``write`` and credited when the receiving end hands them up,
    and crossing a watermark pauses or resumes the protocol synchronously.
    None of it applies until ``net.set_flow_control()`` arms it.
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
        self._write_buffer = 0
        self._protocol_paused = False
        # None means this transport never set its own, so the network default
        # applies — including a change to it made after this transport existed.
        self._limits: tuple[int, int] | None = None
        self._extra_socket: _SimSocket | None = None
        self._peer_closed = False  # the peer's FIN or RST has arrived

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
            # The ports address the peer's end of this connection: on a
            # self-connection the two ends share a host and only the port
            # tells the registry which one a packet is for.
            src_port=self._local[1],
            dst_port=self._remote[1],
        )

    def write(self, data: Any) -> None:
        payload = _check_bytes(data)
        if self._eof_sent or self._closing or self._closed:
            raise RuntimeError("Cannot write to closing transport")
        if payload:
            self._send("data", payload)
            self._charge(len(payload))

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
        # Nothing can be owed on a connection that no longer exists. The
        # protocol is not resumed here: connection_lost is what wakes a writer
        # waiting in drain(), and resume_writing on a torn-down protocol would
        # be a second wakeup the stdlib never sends.
        self._write_buffer = 0
        self._protocol_paused = False
        self._net._drop_stream(self._conn, self._local[0], self._local[1])
        if self._extra_socket is not None:
            self._extra_socket._dispose()
        protocol, self._protocol = self._protocol, None
        if protocol is not None:
            protocol.connection_lost(exc)

    # ------------------------------------------------------------------
    # Write flow control (bytes the peer's protocol has not received yet)
    # ------------------------------------------------------------------

    def _effective_limits(self) -> tuple[int, int]:
        if self._limits is not None:
            return self._limits
        return self._net._flow_defaults

    def _charge(self, count: int) -> None:
        if not self._net._flow_control:
            return
        self._write_buffer += count
        self._maybe_pause_protocol()

    def _credit(self, count: int) -> None:
        if not self._net._flow_control or self._closed:
            return
        # Arming mid-run cannot un-send what is already on the wire, so a
        # credit for bytes that were never charged stops at zero.
        self._write_buffer = max(0, self._write_buffer - count)
        self._maybe_resume_protocol()

    def _consumed(self, count: int) -> None:
        """Release the sender of bytes this end has just handed to its protocol."""
        if not self._net._flow_control:
            return
        # The remote endpoint's own key: on a self-connection the two ends
        # share a host, and the port is what addresses the one that wrote.
        peer = self._net._streams.get(
            (self._conn, self._remote[0], self._remote[1])
        )
        if peer is not None:
            peer._credit(count)

    def _maybe_pause_protocol(self) -> None:
        _low, high = self._effective_limits()
        if self._write_buffer <= high or self._protocol_paused:
            return
        if self._protocol is None:
            return
        self._protocol_paused = True
        try:
            self._protocol.pause_writing()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._report_failure("protocol.pause_writing() failed", exc)

    def _maybe_resume_protocol(self) -> None:
        low, _high = self._effective_limits()
        if not self._protocol_paused or self._write_buffer > low:
            return
        self._protocol_paused = False
        if self._protocol is None:
            return
        try:
            self._protocol.resume_writing()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._report_failure("protocol.resume_writing() failed", exc)

    def _release_flow_control(self) -> None:
        """Let go of a paused writer because the switch just went off."""
        self._write_buffer = 0
        if not self._protocol_paused:
            return
        self._protocol_paused = False
        if self._protocol is None:
            return
        try:
            self._protocol.resume_writing()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._report_failure("protocol.resume_writing() failed", exc)

    def _report_failure(self, message: str, exc: BaseException) -> None:
        self._net._loop.call_exception_handler(
            {
                "message": message,
                "exception": exc,
                "transport": self,
                "protocol": self._protocol,
            }
        )

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
        # Credited after the call, not before: the sender is released only once
        # the receiving protocol has finished with the bytes.
        self._consumed(len(data))

    def _eof_arrived(self) -> None:
        self._peer_closed = True
        if self._extra_socket is not None:
            self._extra_socket._peer_gone()
        if self._closed:
            return
        if self._read_paused:
            self._eof_pending = True
            return
        keep_open = self._protocol.eof_received()
        if not keep_open:
            self.close()

    def _reset_arrived(self) -> None:
        # No _peer_gone() here: a reset tears the connection down on the spot,
        # and _finish closes both parked ends. The descriptor goes to -1
        # rather than turning readable, which a liveness poll reads the same
        # way — as a connection to discard.
        self._peer_closed = True
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
            chunk = self._backlog.pop(0)
            self._protocol.data_received(chunk)
            # Per chunk, so a protocol that pauses again mid-drain leaves the
            # rest of the backlog charged to the sender.
            self._consumed(len(chunk))
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
        if name == "socket":
            if self._extra_socket is None:
                self._extra_socket = _SimSocket(self)
                if self._closed:
                    # Asked for after teardown: born closed, like the socket a
                    # finished connection leaves behind. Nothing here may hand
                    # out a live descriptor for a connection that is gone.
                    self._extra_socket._dispose()
            return self._extra_socket
        return default

    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol

    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        self._limits = _check_limits(high, low)
        # Only the pause side, matching the stdlib: lowering the marks under a
        # full buffer pauses at once, raising them waits for a read to resume.
        self._maybe_pause_protocol()

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return self._effective_limits()

    def get_write_buffer_size(self) -> int:
        # With flow control off nothing is charged and writes leave
        # immediately, so nothing is buffered to report.
        return self._write_buffer if self._net._flow_control else 0
