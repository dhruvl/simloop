"""In-memory network of named hosts for code running under a SimLoop.

Tasks are pinned to hosts through a context variable: a task started via
``Host.create_task`` — and every task it spawns — carries that host's name,
which is how the network attributes traffic to a source machine and how a
crash knows which tasks to kill. Tasks created outside any host belong to
an implicit ``driver`` host, so test glue needs no ceremony.
"""

from __future__ import annotations

import random
import socket
from collections.abc import Iterable, Iterator, MutableMapping
from contextvars import Context, ContextVar, copy_context
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncio

from simloop._transports import _SimDatagramTransport, _SimStreamTransport

if TYPE_CHECKING:
    from simloop._loop import SimLoop

DRIVER = "driver"

# What runs under no machine at all: the network's own delivery step, which
# belongs to the wire between two hosts rather than to either of them. No
# registered host can be named this (``SimNetwork.host`` rejects an empty
# name), so it can never be confused with one, and it is exactly what an empty
# host field means in the trace.
WIRE = ""

_current_host: ContextVar[str] = ContextVar("simloop_current_host", default=DRIVER)

# Host names appear inside trace labels, whose hash serialization relies on
# "|" and newline never occurring in a label; ">" is the separator inside
# network labels themselves.
_FORBIDDEN_NAME_CHARS = ("|", "\n", ">")

# Synthetic addresses come out of 10.7.0.0/16: a private range, so a leaked
# address can never be routed anywhere real, and wide enough that the offset
# from the base is simply the host's registration index.
_ADDRESS_BASE = 0x0A070000
_ADDRESS_LIMIT = 0xFFFE

# Names production code writes to mean "the machine I am running on".
_LOCAL_NAMES = ("", "0.0.0.0", "localhost", "127.0.0.1")

# One row per socket kind the network actually implements.
_SOCKET_KINDS = (
    (socket.SOCK_STREAM, socket.IPPROTO_TCP),
    (socket.SOCK_DGRAM, socket.IPPROTO_UDP),
)

_AddrInfo = tuple[int, int, int, str, tuple[str, int]]


def _format_address(index: int) -> str:
    packed = _ADDRESS_BASE + index
    return ".".join(str((packed >> shift) & 0xFF) for shift in (24, 16, 8, 0))


def _wire_context() -> Context:
    """A context in which no machine owns the running callback.

    A delivery is scheduled by whichever machine sent the packet, so a handle
    left to copy the ambient context would make the trace read as if the
    sender ran the delivery step. It did not: crossing the wire is the
    simulation's work, and ``_deliver`` pins the *receiving* host itself before
    handing the packet up. Everything else the caller's context carries is
    copied through untouched.
    """
    token = _current_host.set(WIRE)
    try:
        return copy_context()
    finally:
        _current_host.reset(token)


def _gaierror(what: Any) -> socket.gaierror:
    # EAI_NONAME is what a real resolver returns for a name that does not
    # exist, so callers that special-case it keep working under simulation.
    return socket.gaierror(socket.EAI_NONAME, f"Name or service not known: {what!r}")


def _decoded(host: Any) -> Any:
    # The stdlib resolver accepts ASCII bytes for host names, and resolver
    # stacks lean on it: anyio IDNA-encodes every name before calling
    # getaddrinfo, so bytes must mean here what they mean there. Bytes that
    # are not ASCII could never name a registered host; everything else
    # passes through for the caller's own validation to judge.
    if isinstance(host, (bytes, bytearray)):
        try:
            return bytes(host).decode("ascii")
        except UnicodeDecodeError:
            return host
    return host


def _service_port(port: Any) -> int:
    # Numeric services only: a service-name database is one more thing that
    # would differ between machines, and nothing in the simulation needs it.
    if port is None:
        return 0
    if isinstance(port, int) and not isinstance(port, bool):
        return port
    if isinstance(port, str) and port.isdigit():
        return int(port)
    raise _gaierror(port)


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

    @property
    def sockets(self) -> tuple[Any, ...]:
        """Always empty: a simulated server owns no operating-system sockets.

        Server libraries read this attribute to report which address they
        bound — aiohttp's runner and websockets' serve loop both touch it
        during startup — and they already handle a server with no sockets,
        because the stdlib documents the tuple as possibly empty. Absent,
        the attribute error aborts startup; empty, startup proceeds and the
        simulation stays honest about what a listener here really is.
        """
        return ()

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


class SimDisk(MutableMapping[str, object]):
    """A host's storage that survives crashes and restarts.

    A crash loses everything volatile — tasks, connections, binds — but not
    what was written here, which is the whole point: this is where state
    that a real process would fsync belongs. Writes are atomic at
    assignment; there is no partial-write model. Values are stored as
    given, so mutating a stored object later is the caller's own aliasing,
    exactly as it would be with a cache in front of a real disk.
    """

    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __setitem__(self, key: str, value: object) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class Host:
    """Handle for one simulated machine; tasks started here are pinned to it."""

    def __init__(self, net: SimNetwork, name: str) -> None:
        self._net = net
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def disk(self) -> SimDisk:
        return self._net._disks.setdefault(self._name, SimDisk())

    def create_task(self, coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        token = _current_host.set(self._name)
        try:
            return self._net._loop.create_task(coro, name=name)
        finally:
            _current_host.reset(token)

    def crash(self) -> None:
        self._net.crash(self._name)

    def restart(self) -> None:
        self._net.restart(self._name)


class SimNetwork:
    """Registry of hosts and the traffic between them."""

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        # Fault decisions draw from their own seed-derived stream so they can
        # never perturb the scheduler's draws or the sim.* user streams.
        self._rng = random.Random(f"{loop.seed}:net")
        self._hosts: dict[str, Host] = {}
        self._addresses: dict[str, str] = {}
        self._names: dict[str, str] = {}
        self._alive: dict[str, bool] = {}
        self._disks: dict[str, SimDisk] = {}
        self._clock_offsets: dict[str, float] = {}
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
        index = len(self._addresses) + 1
        if index > _ADDRESS_LIMIT:
            raise ValueError(
                f"the simulated address range holds at most {_ADDRESS_LIMIT} hosts"
            )
        host = Host(self, name)
        self._hosts[name] = host
        address = _format_address(index)
        self._addresses[name] = address
        self._names[address] = name
        self._alive[name] = True
        self._tasks[name] = []
        return host

    def address(self, name: str) -> str:
        """The synthetic IPv4 address a host was given when it registered.

        Addresses are handed out in registration order, so a run's addressing
        is as reproducible as everything else in the simulation.
        """
        self._require_host(name)
        return self._addresses[name]

    def hostname(self, address: str) -> str:
        """The host owning a synthetic address; the inverse of ``address``."""
        name = self._names.get(address)
        if name is None:
            raise OSError(f"unknown address {address!r}")
        return name

    # ------------------------------------------------------------------
    # Name resolution
    # ------------------------------------------------------------------

    def _resolve(self, host: Any) -> str:
        """Map an endpoint address to the host name the packet layer uses.

        Synthetic addresses are accepted anywhere a name is, so code that
        resolved a name through ``getaddrinfo`` can connect to what it got
        back without knowing it is talking to a simulated network.
        """
        host = _decoded(host)
        if isinstance(host, str):
            name = self._names.get(host)
            if name is not None:
                return name
        return self._require_host(host)

    def _lookup_address(self, host: Any) -> str:
        host = _decoded(host)
        if host is None or (isinstance(host, str) and host in _LOCAL_NAMES):
            # Loopback-shaped names mean the calling task's own machine, the
            # same reading _bind_address gives them.
            return self._addresses[_current_host.get()]
        if not isinstance(host, str):
            raise _gaierror(host)
        if host in self._names:
            return host
        address = self._addresses.get(host)
        if address is None:
            raise _gaierror(host)
        return address

    def _getaddrinfo(
        self, host: Any, port: Any, family: int, type: int, proto: int, flags: int
    ) -> list[_AddrInfo]:
        # Resolver flags (AI_PASSIVE, AI_CANONNAME, ...) have nothing to vary
        # here: one address per host, and no canonical names to report.
        address = self._lookup_address(host)
        number = _service_port(port)
        rows: list[_AddrInfo] = [
            (socket.AF_INET, kind, protocol, "", (address, number))
            for kind, protocol in _SOCKET_KINDS
            if type in (0, kind) and proto in (0, protocol)
        ]
        if family not in (0, socket.AF_INET) or not rows:
            # Sim hosts are IPv4-only and speak TCP or UDP, so a request for
            # anything else has no answer at all rather than a partial one.
            raise _gaierror(host)
        return rows

    def _getnameinfo(self, sockaddr: Any, flags: int) -> tuple[str, str]:
        if not isinstance(sockaddr, tuple) or len(sockaddr) < 2:
            raise _gaierror(sockaddr)
        address, port = sockaddr[0], sockaddr[1]
        if not isinstance(address, str):
            raise _gaierror(address)
        if address in self._names:
            name, numeric = self._names[address], address
        elif address in self._addresses:
            # Peer addresses inside the simulation are host names, so a caller
            # can hand one straight back from get_extra_info("peername").
            name, numeric = address, self._addresses[address]
        else:
            raise _gaierror(address)
        resolved = numeric if flags & socket.NI_NUMERICHOST else name
        return (resolved, str(_service_port(port)))

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

    def set_clock(self, name: str, *, offset: float) -> None:
        """Skew what a host's tasks read from the clock, in seconds.

        Offset changes what ``loop.time()`` *reads* on that host — never how
        long a duration takes: ``asyncio.sleep(1.0)`` still costs one true
        second everywhere, which is what a wrong wall clock does on a real
        machine. Deadlines passed to ``call_at`` are interpreted on the
        calling task's clock. By default the driver and unconfigured hosts
        read true time.
        """
        self._require_host(name)
        self._clock_offsets[name] = float(offset)

    def clock_offset(self, name: str) -> float:
        self._require_host(name)
        return self._clock_offsets.get(name, 0.0)

    def _offset_now(self) -> float:
        return self._clock_offsets.get(_current_host.get(), 0.0)

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
        # Trace timestamps are always the true clock, never the calling
        # host's: a packet event happens once, at one shared instant, and
        # traces from differently-skewed runs have to stay comparable.
        # _deliver and the transports run under a host context, so
        # loop.time() here would read that host's skewed clock.
        self._loop._recorder.record(
            "net",
            self._loop._true_time(),
            packet.uid,
            f"{verb} {packet.src}>{packet.dst}",
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
        # The schedule event still names the sender — call_later reads the live
        # context, which the wire context is built and discarded around — while
        # the delivery step itself is attributed to no machine.
        self._loop.call_later(delay, self._deliver, packet, context=_wire_context())

    def _deliver(self, packet: _Packet) -> None:
        if not (self._alive[packet.src] and self._alive[packet.dst]):
            self._trace("lost", packet)
            return
        if self._is_cut(packet.src, packet.dst):
            # The cut appeared while this packet was in flight.
            self._blackhole(packet)
            return
        # The packet reached the destination machine: every "send" that is not
        # answered by a drop, hold or loss is answered by exactly this, which
        # is what turns the packet log into a set of arrows with two ends.
        # What the machine then does with it — hand it to a socket, discard it
        # for want of one, hold it for an earlier sequence — is its own affair.
        self._trace("deliver", packet)
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
        host = _decoded(host)
        if not isinstance(host, str) or not isinstance(port, int):
            raise ValueError("host and port are required")
        host = self._resolve(host)
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
        if host is None or host in _LOCAL_NAMES:
            # Production-shaped bind addresses mean "this machine": the host
            # the calling task is pinned to.
            return (owner, port)
        if self._names.get(host, host) != owner:
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
            remote = (self._resolve(remote_addr[0]), remote_addr[1])
        transport = _SimDatagramTransport(self, bind, remote)
        self._datagrams[bind] = transport
        protocol = protocol_factory()
        transport._begin(protocol)
        return transport, protocol

    def _send_datagram(
        self, src: tuple[str, int], dst: tuple[str, int], payload: bytes
    ) -> None:
        self._transmit(
            _Packet(
                kind="dgram",
                src=src[0],
                dst=self._resolve(dst[0]),
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
        self._loop._recorder.record(
            "net", self._loop._true_time(), self._new_uid(), f"crash {name}"
        )

    def restart(self, name: str) -> None:
        """Bring a crashed host back as a fresh incarnation.

        Restart revives liveness and nothing else: the old incarnation's
        tasks are already cancelled, its listeners and binds are gone, and
        its stream connections are dead — packets addressed to them vanish,
        so peers still learn about the outage only from their own timeouts.
        State meant to survive the reboot belongs on ``Host.disk``. The
        caller boots whatever should run on the revived machine, the same
        way it booted the machine the first time.

        "Already cancelled" means requested, not finished: ``crash`` asks
        each task to cancel and the cancellation lands on the next
        scheduler step, so a restart in the same step can briefly coexist
        with a dying task that swallows ``CancelledError``.
        """
        self._require_host(name)
        if self._alive[name]:
            raise ValueError(f"host {name!r} is not crashed")
        self._alive[name] = True
        self._loop._recorder.record(
            "net", self._loop._true_time(), self._new_uid(), f"restart {name}"
        )

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        owner = self._tasks[_current_host.get()]
        owner.append(task)
        task.add_done_callback(owner.remove)

    def _require_host(self, name: str) -> str:
        if name not in self._hosts:
            raise OSError(f"unknown host {name!r}")
        return name
