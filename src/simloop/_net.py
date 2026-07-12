"""In-memory network of named hosts for code running under a SimLoop.

Tasks are pinned to hosts through a context variable: a task started via
``Host.create_task`` — and every task it spawns — carries that host's name,
which is how the network attributes traffic to a source machine and how a
crash knows which tasks to kill. Tasks created outside any host belong to
an implicit ``driver`` host, so test glue needs no ceremony.
"""

from __future__ import annotations

import random
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncio

from simloop._transports import _SimDatagramTransport

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
        self._datagrams: dict[tuple[str, int], _SimDatagramTransport] = {}
        self._next_uid = 0
        self._next_port = 49152
        self.host(DRIVER)

    def host(self, name: str) -> Host:
        if not name:
            raise ValueError("host name must be a non-empty string")
        if any(ch in name for ch in _FORBIDDEN_NAME_CHARS):
            raise ValueError(f"host name {name!r} may not contain '|', '>' or newline")
        if name in self._hosts:
            raise ValueError(f"host {name!r} already exists")
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
        raise NotImplementedError

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
        raise NotImplementedError

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        owner = self._tasks[_current_host.get()]
        owner.append(task)
        task.add_done_callback(owner.remove)

    def _require_host(self, name: str) -> str:
        if name not in self._hosts:
            raise OSError(f"unknown host {name!r}")
        return name
