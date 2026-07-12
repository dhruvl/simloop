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
from typing import TYPE_CHECKING, Any

import asyncio

if TYPE_CHECKING:
    from simloop._loop import SimLoop

DRIVER = "driver"

_current_host: ContextVar[str] = ContextVar("simloop_current_host", default=DRIVER)

# Host names appear inside trace labels, whose hash serialization relies on
# "|" and newline never occurring in a label; ">" is the separator inside
# network labels themselves.
_FORBIDDEN_NAME_CHARS = ("|", "\n", ">")


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
