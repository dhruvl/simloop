"""In-memory network of named hosts for code running under a SimLoop.

Tasks are pinned to hosts through a context variable: a task started via
``Host.create_task`` — and every task it spawns — carries that host's name,
which is how the network attributes traffic to a source machine and how a
crash knows which tasks to kill. Tasks created outside any host belong to
an implicit ``driver`` host, so test glue needs no ceremony.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncio

from simloop._transports import _SimDatagramTransport, _SimStreamTransport

if TYPE_CHECKING:
    from simloop._loop import SimLoop

DRIVER = "driver"

_current_host: ContextVar[str] = ContextVar("simloop_current_host", default=DRIVER)

# Host names appear inside trace labels, whose hash serialization relies on
# "|" and newline never occurring in a label; ">" is the separator inside
# network labels themselves.
_FORBIDDEN_NAME_CHARS = ("|", "\n", ">")


@dataclass(slots=True)
class _Packet:
    kind: str  # "dgram", "syn", "accept", "refuse", "data", "fin", "rst"
    src: str
    dst: str
    src_port: int
    dst_port: int
    conn: int  # connection id; -1 for datagrams
    seq: int  # per-direction stream sequence; -1 for datagrams
    payload: bytes
    uid: int


@dataclass(slots=True)
class _Link:
    latency: tuple[float, float] | None = None
    drop: float | None = None
    duplicate: float | None = None


@dataclass(slots=True)
class _Listener:
    factory: Any
    server: SimServer


@dataclass(slots=True)
class _Connect:
    # An in-flight outbound connection. The client transport is built when the
    # accept lands (not when the connector resumes), so a peer that speaks
    # first cannot outrun the transport's registration.
    fut: asyncio.Future[tuple[_SimStreamTransport, Any]]
    factory: Any
    local: tuple[str, int]
    remote: tuple[str, int]


class _InOrder:
    """Reassembles one direction of a stream connection into seq order."""

    def __init__(self, net: SimNetwork) -> None:
        self._net = net
        self._next = 0
        self._early: dict[int, _Packet] = {}

    def push(self, packet: _Packet) -> None:
        self._early[packet.seq] = packet
        while self._next in self._early:
            ready = self._early.pop(self._next)
            self._next += 1
            self._net._dispatch_ready(ready)


class SimServer(asyncio.AbstractServer):
    """A listening endpoint; serving from creation, like the stdlib default."""

    def __init__(self, net: SimNetwork, host: str, port: int) -> None:
        self._net = net
        self._host = host
        self._port = port
        self._closed_fut: asyncio.Future[None] = net._loop.create_future()

    def close(self) -> None:
        if not self._closed_fut.done():
            self._net._listeners.pop((self._host, self._port), None)
            self._closed_fut.set_result(None)

    def is_serving(self) -> bool:
        return not self._closed_fut.done()

    def close_clients(self) -> None:
        """Close every connection this server accepted."""
        for transport in list(self._net._streams.values()):
            if transport._local == (self._host, self._port):
                transport.close()

    def abort_clients(self) -> None:
        """Reset every connection this server accepted."""
        for transport in list(self._net._streams.values()):
            if transport._local == (self._host, self._port):
                transport.abort()

    async def wait_closed(self) -> None:
        await asyncio.shield(self._closed_fut)

    async def start_serving(self) -> None:
        return None

    async def serve_forever(self) -> None:
        await asyncio.shield(self._closed_fut)

    def get_loop(self) -> Any:
        return self._net._loop


def _check_probability(name: str, value: float) -> float:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be within [0.0, 1.0], got {value!r}")
    return value


def _check_latency(value: tuple[float, float]) -> tuple[float, float]:
    lo, hi = value
    if lo < 0.0 or hi < lo:
        raise ValueError(f"latency must satisfy 0 <= lo <= hi, got {value!r}")
    return (lo, hi)


class Host:
    """Handle for one simulated machine; tasks started here are pinned to it."""

    def __init__(self, net: SimNetwork, name: str) -> None:
        self._net = net
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def create_task(self, coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        token = _current_host.set(self._name)
        try:
            return self._net._loop.create_task(coro, name=name)
        finally:
            _current_host.reset(token)

    def crash(self) -> None:
        self._net.crash(self._name)


class SimNetwork:
    """Registry of hosts and the traffic between them."""

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        # Fault decisions draw from their own seed-derived stream so they can
        # never perturb the scheduler's draws or the sim.* user streams.
        self._rng = random.Random(f"{loop.seed}:net")
        self._hosts: dict[str, Host] = {}
        self._alive: dict[str, bool] = {}
        self._tasks: dict[str, list[asyncio.Task[Any]]] = {}
        self._default_latency: tuple[float, float] = (0.0, 0.0)
        self._default_drop = 0.0
        self._default_duplicate = 0.0
        self._links: dict[tuple[str, str], _Link] = {}
        self._cuts: set[frozenset[str]] = set()
        self._held: list[_Packet] = []
        self._datagrams: dict[tuple[str, int], _SimDatagramTransport] = {}
        self._listeners: dict[tuple[str, int], _Listener] = {}
        self._streams: dict[tuple[int, str], _SimStreamTransport] = {}
        self._inbound: dict[tuple[int, str], _InOrder] = {}
        self._pending: dict[int, _Connect] = {}
        self._next_conn = 0
        self._next_uid = 0
        self._next_port = 49152
        self.host(DRIVER)

    def host(self, name: str) -> Host:
        existing = self._hosts.get(name)
        if existing is not None:
            return existing
        if not name:
            raise ValueError("host name must be a non-empty string")
        if any(ch in name for ch in _FORBIDDEN_NAME_CHARS):
            raise ValueError(f"host name {name!r} may not contain '|', '>' or newline")
        host = Host(self, name)
        self._hosts[name] = host
        self._alive[name] = True
        self._tasks[name] = []
        return host

    def set_defaults(
        self,
        *,
        latency: tuple[float, float] | None = None,
        drop: float | None = None,
        duplicate: float | None = None,
    ) -> None:
        if latency is not None:
            self._default_latency = _check_latency(latency)
        if drop is not None:
            self._default_drop = _check_probability("drop", drop)
        if duplicate is not None:
            self._default_duplicate = _check_probability("duplicate", duplicate)

    def set_link(
        self,
        src: str,
        dst: str,
        *,
        latency: tuple[float, float] | None = None,
        drop: float | None = None,
        duplicate: float | None = None,
    ) -> None:
        self._require_host(src)
        self._require_host(dst)
        link = self._links.setdefault((src, dst), _Link())
        if latency is not None:
            link.latency = _check_latency(latency)
        if drop is not None:
            link.drop = _check_probability("drop", drop)
        if duplicate is not None:
            link.duplicate = _check_probability("duplicate", duplicate)

    def partition(self, group_a: Iterable[str], group_b: Iterable[str]) -> None:
        side_a = [self._require_host(name) for name in group_a]
        side_b = [self._require_host(name) for name in group_b]
        if not side_a or not side_b:
            raise ValueError("both partition groups must be non-empty")
        overlap = set(side_a) & set(side_b)
        if overlap:
            raise ValueError(
                f"hosts cannot be on both sides of a partition: {sorted(overlap)}"
            )
        for a in side_a:
            for b in side_b:
                self._cuts.add(frozenset((a, b)))

    def heal(self) -> None:
        self._cuts.clear()
        held, self._held = self._held, []
        for packet in held:
            self._trace("release", packet)
            self._transmit(packet)

    def _is_cut(self, a: str, b: str) -> bool:
        return frozenset((a, b)) in self._cuts

    def _blackhole(self, packet: _Packet) -> None:
        # Datagrams crossing a cut are simply gone. Stream packets are held
        # and released on heal: with no retransmission model, permanently
        # dropping a mid-stream packet would leave the receiver waiting on a
        # sequence gap forever, so held-then-released is what "the bytes stop
        # flowing, then the connection resumes intact" has to mean here.
        if packet.kind == "dgram":
            self._trace("drop", packet)
        else:
            self._held.append(packet)
            self._trace("hold", packet)

    def _resolved(self, src: str, dst: str) -> tuple[tuple[float, float], float, float]:
        link = self._links.get((src, dst))
        if link is None:
            return (self._default_latency, self._default_drop, self._default_duplicate)
        return (
            link.latency if link.latency is not None else self._default_latency,
            link.drop if link.drop is not None else self._default_drop,
            link.duplicate if link.duplicate is not None else self._default_duplicate,
        )

    # ------------------------------------------------------------------
    # Packet pipeline
    # ------------------------------------------------------------------

    def _new_uid(self) -> int:
        uid = self._next_uid
        self._next_uid += 1
        return uid

    def _ephemeral(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port

    def _trace(self, verb: str, packet: _Packet) -> None:
        self._loop._recorder.record(
            "net", self._loop.time(), packet.uid, f"{verb} {packet.src}>{packet.dst}"
        )

    def _transmit(self, packet: _Packet) -> None:
        if self._is_cut(packet.src, packet.dst):
            self._blackhole(packet)
            return
        latency, drop, duplicate = self._resolved(packet.src, packet.dst)
        if packet.kind == "dgram":
            # Only datagrams are lossy: a reliable stream that loses bytes
            # would be lying about being a stream.
            if self._rng.random() < drop:
                self._trace("drop", packet)
                return
            if self._rng.random() < duplicate:
                self._trace("dup", packet)
                self._schedule(packet, latency)
        self._schedule(packet, latency)

    def _schedule(self, packet: _Packet, latency: tuple[float, float]) -> None:
        self._trace("send", packet)
        delay = self._rng.uniform(latency[0], latency[1])
        self._loop.call_later(delay, self._deliver, packet)

    def _deliver(self, packet: _Packet) -> None:
        if not (self._alive[packet.src] and self._alive[packet.dst]):
            self._trace("lost", packet)
            return
        if self._is_cut(packet.src, packet.dst):
            # The cut appeared while this packet was in flight.
            self._blackhole(packet)
            return
        # Anything the receiving protocol schedules (including tasks spawned
        # from connection_made or datagram_received) must be pinned to the
        # receiving host, not to whichever context sent the packet.
        token = _current_host.set(packet.dst)
        try:
            if packet.kind == "dgram":
                transport = self._datagrams.get((packet.dst, packet.dst_port))
                if transport is None:
                    self._trace("lost", packet)
                    return
                transport._datagram_arrived(packet.payload, (packet.src, packet.src_port))
            else:
                self._dispatch_stream(packet)
        finally:
            _current_host.reset(token)

    def _dispatch_stream(self, packet: _Packet) -> None:
        key = (packet.conn, packet.dst)
        queue = self._inbound.get(key)
        if queue is None:
            queue = self._inbound[key] = _InOrder(self)
        queue.push(packet)

    def _dispatch_ready(self, packet: _Packet) -> None:
        if packet.kind == "syn":
            self._handle_syn(packet)
            return
        if packet.kind in ("accept", "refuse"):
            connect = self._pending.pop(packet.conn, None)
            if connect is None or connect.fut.done():
                # The connector gave up (cancelled) before the answer landed.
                return
            if packet.kind == "accept":
                # Stand the client transport up now, in the same in-order step
                # that processes the accept (seq 0). Data the peer sent from
                # connection_made is seq 1+, so it is dispatched strictly after
                # this and always finds a registered transport.
                client = _SimStreamTransport(
                    self, packet.conn, local=connect.local, remote=connect.remote
                )
                self._streams[(packet.conn, connect.local[0])] = client
                protocol = connect.factory()
                client._begin(protocol)
                connect.fut.set_result((client, protocol))
            else:
                connect.fut.set_exception(
                    ConnectionRefusedError(
                        f"connect to ({packet.src!r}, {packet.dst_port}) refused"
                    )
                )
            return
        transport = self._streams.get((packet.conn, packet.dst))
        if transport is None:
            return  # connection already torn down locally
        if packet.kind == "data":
            transport._data_arrived(packet.payload)
        elif packet.kind == "fin":
            transport._eof_arrived()
        elif packet.kind == "rst":
            transport._reset_arrived()

    def _handle_syn(self, packet: _Packet) -> None:
        listener = self._listeners.get((packet.dst, packet.dst_port))
        if listener is None:
            self._send_stream(
                kind="refuse",
                src=packet.dst,
                dst=packet.src,
                conn=packet.conn,
                seq=0,
                dst_port=packet.dst_port,
            )
            return
        transport = _SimStreamTransport(
            self,
            packet.conn,
            local=(packet.dst, packet.dst_port),
            remote=(packet.src, packet.src_port),
        )
        self._streams[(packet.conn, packet.dst)] = transport
        # The accept is seq 0 of the server-to-client direction, so any data
        # the protocol writes from connection_made (seq 1+) can never arrive
        # ahead of the accept, whatever the latency draws say.
        self._send_stream(
            kind="accept", src=packet.dst, dst=packet.src, conn=packet.conn, seq=0
        )
        protocol = listener.factory()
        transport._begin(protocol)

    def _send_stream(
        self,
        *,
        kind: str,
        src: str,
        dst: str,
        conn: int,
        seq: int,
        payload: bytes = b"",
        src_port: int = 0,
        dst_port: int = 0,
    ) -> None:
        self._transmit(
            _Packet(
                kind=kind,
                src=src,
                dst=dst,
                src_port=src_port,
                dst_port=dst_port,
                conn=conn,
                seq=seq,
                payload=payload,
                uid=self._new_uid(),
            )
        )

    def _drop_stream(self, conn: int, host: str) -> None:
        self._streams.pop((conn, host), None)

    async def _open_connection(
        self, protocol_factory: Any, host: Any, port: Any
    ) -> tuple[_SimStreamTransport, Any]:
        if not isinstance(host, str) or not isinstance(port, int):
            raise ValueError("host and port are required")
        self._require_host(host)
        src = _current_host.get()
        conn = self._next_conn
        self._next_conn += 1
        src_port = self._ephemeral()
        fut: asyncio.Future[tuple[_SimStreamTransport, Any]] = (
            self._loop.create_future()
        )
        self._pending[conn] = _Connect(
            fut=fut,
            factory=protocol_factory,
            local=(src, src_port),
            remote=(host, port),
        )
        self._send_stream(
            kind="syn",
            src=src,
            dst=host,
            conn=conn,
            seq=0,
            src_port=src_port,
            dst_port=port,
        )
        try:
            # The accept handler builds the transport, calls connection_made,
            # and resolves this future with the ready-made pair.
            return await fut
        except asyncio.CancelledError:
            # If the accept resolved this future in the same step the connector
            # was cancelled, the transport is already built and connection_made
            # has already run; abort it so a cancelled connect leaves nothing
            # connected behind it.
            if fut.done() and not fut.cancelled() and fut.exception() is None:
                established, _ = fut.result()
                established.abort()
            raise
        finally:
            self._pending.pop(conn, None)

    async def _start_server(
        self, protocol_factory: Any, host: Any, port: Any
    ) -> SimServer:
        if not isinstance(port, int):
            raise ValueError("port is required")
        bind = self._bind_address(host, port)
        if bind in self._listeners:
            raise OSError(f"address {bind[0]!r}:{bind[1]} already in use")
        server = SimServer(self, bind[0], bind[1])
        self._listeners[bind] = _Listener(factory=protocol_factory, server=server)
        return server

    # ------------------------------------------------------------------
    # Datagram endpoints
    # ------------------------------------------------------------------

    def _bind_address(self, host: str | None, port: int) -> tuple[str, int]:
        owner = _current_host.get()
        if host in (None, "", "0.0.0.0", "localhost", "127.0.0.1"):
            # Production-shaped bind addresses mean "this machine": the host
            # the calling task is pinned to.
            return (owner, port)
        if host != owner:
            raise OSError(f"cannot bind to {host!r} from host {owner!r}")
        return (owner, port)

    async def _open_datagram_endpoint(
        self,
        protocol_factory: Any,
        local_addr: tuple[str, int] | None,
        remote_addr: tuple[str, int] | None,
    ) -> tuple[_SimDatagramTransport, Any]:
        if local_addr is None:
            bind = (_current_host.get(), self._ephemeral())
        else:
            bind = self._bind_address(local_addr[0], local_addr[1])
        if bind in self._datagrams:
            raise OSError(f"address {bind[0]!r}:{bind[1]} already in use")
        remote: tuple[str, int] | None = None
        if remote_addr is not None:
            remote = (self._require_host(remote_addr[0]), remote_addr[1])
        transport = _SimDatagramTransport(self, bind, remote)
        self._datagrams[bind] = transport
        protocol = protocol_factory()
        transport._begin(protocol)
        return transport, protocol

    def _send_datagram(
        self, src: tuple[str, int], dst: tuple[str, int], payload: bytes
    ) -> None:
        self._require_host(dst[0])
        self._transmit(
            _Packet(
                kind="dgram",
                src=src[0],
                dst=dst[0],
                src_port=src[1],
                dst_port=dst[1],
                conn=-1,
                seq=-1,
                payload=payload,
                uid=self._new_uid(),
            )
        )

    def _unbind_datagram(self, addr: tuple[str, int]) -> None:
        self._datagrams.pop(addr, None)

    def crash(self, name: str) -> None:
        """Kill a host mid-run: its tasks are cancelled and it goes silent.

        A crashed machine sends no reset — peers see nothing at all, which is
        what makes crashes indistinguishable from partitions to the code
        under test until a timeout says otherwise.
        """
        self._require_host(name)
        if name == DRIVER:
            raise ValueError("the driver host cannot crash")
        if not self._alive[name]:
            raise ValueError(f"host {name!r} already crashed")
        self._alive[name] = False
        for task in list(self._tasks[name]):
            task.cancel()
        for key in [key for key in self._listeners if key[0] == name]:
            self._listeners[key].server.close()
        for key in [key for key in self._datagrams if key[0] == name]:
            del self._datagrams[key]
        # Tear down this host's own stream transports in-band: _finish sends no
        # packet (a crashed host stays silent), pops the transport from
        # _streams, and delivers connection_lost(None), so cleanup never falls
        # to garbage collection. The materialized key list keeps _drop_stream's
        # mutation from disturbing the iteration. The peer's transport for the
        # same connection is left alone: it is still alive and times out itself.
        for stream_key in [sk for sk in self._streams if sk[1] == name]:
            self._streams[stream_key]._finish(None)
        kept: list[_Packet] = []
        for packet in self._held:
            if name in (packet.src, packet.dst):
                self._trace("lost", packet)
            else:
                kept.append(packet)
        self._held = kept

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        owner = self._tasks[_current_host.get()]
        owner.append(task)
        task.add_done_callback(owner.remove)

    def _require_host(self, name: str) -> str:
        if name not in self._hosts:
            raise OSError(f"unknown host {name!r}")
        return name
