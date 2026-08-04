"""A deterministic asyncio event loop with a virtual clock and seeded scheduling."""

from __future__ import annotations

import asyncio
import gc
import heapq
import random
import socket
import sys
import threading
import weakref
from array import array
from asyncio import events
from collections.abc import Callable, Iterable, Sequence
from contextvars import Context
from typing import TYPE_CHECKING, Any, NoReturn, TypeVarTuple, Unpack, overload

if TYPE_CHECKING:
    from asyncio.events import _TaskFactory

from simloop._net import DRIVER, SimNetwork, _current_host
from simloop._policy import (
    CHOICE_TYPECODE,
    MAX_CHOICE,
    ReadyView,
    SchedulingPolicy,
    ScriptedPolicy,
    SeededPolicy,
)
from simloop._trace import TraceEvent, TraceRecorder

_Ts = TypeVarTuple("_Ts")

_ExceptionHandler = Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object]


class SimulationDeadlockError(RuntimeError):
    """No runnable callbacks or timers remain, but the awaited future is not done.

    This usually means a lost wakeup: some task is waiting on a future or queue
    that nothing will ever complete.
    """


class SimulationFenceError(NotImplementedError):
    """The code under simulation touched an asyncio API simloop does not simulate.

    Real I/O, threads, signals and subprocesses reach outside the simulation,
    so they fail loudly instead of silently breaking determinism.
    """


def _fence(api: str) -> NoReturn:
    raise SimulationFenceError(
        f"simloop does not simulate {api!r}; "
        "see docs/supported-api.md for the supported asyncio subset"
    )


def _reject_kwargs(api: str, kwargs: dict[str, Any]) -> None:
    # Optional stdlib arguments (ssl, sock, interface selectors, ...) reach
    # outside the simulation; anything actually requested must fail loudly.
    for name, value in kwargs.items():
        if value:
            _fence(f"{api}({name}=...)")


def _label(callback: Callable[..., object]) -> str:
    # Labels feed the trace hash, so they must be stable across processes:
    # qualified names only, never repr() (which can embed memory addresses).
    name = getattr(callback, "__qualname__", None)
    if isinstance(name, str):
        return name
    return type(callback).__name__


class _ExecutorJob:
    """One ``run_in_executor`` submission, run inline at its scheduled step.

    The instance carries the submitted function's qualified name as its own
    ``__qualname__``, so the trace labels the step with the work rather than
    the wrapper. Outcomes land on the future the way an executor worker would
    land them: any ``BaseException`` is stored rather than raised, and a
    future already cancelled when the step runs means the function never runs
    at all — the inline equivalent of cancelling a pending work item.
    """

    def __init__(
        self,
        func: Callable[..., object],
        args: tuple[Any, ...],
        future: asyncio.Future[Any],
    ) -> None:
        self._func = func
        self._args = args
        self._future = future
        self.__qualname__ = f"executor:{_label(func)}"

    def __call__(self) -> None:
        future = self._future
        if future.cancelled():
            return
        try:
            result = self._func(*self._args)
        except BaseException as exc:
            future.set_exception(exc)
        else:
            future.set_result(result)


def _host_of(handle: asyncio.Handle) -> str:
    """The simulated machine whose code this handle will run.

    asyncio copies the scheduling context into every handle, and a task hands
    its own context to each of its steps, so the pinned host travels with the
    callback rather than with whoever happened to wake it: a task woken by a
    packet from another machine is still attributed to its own. Reading it
    here rather than at scheduling time is what makes that true.

    ``Context.get`` looks up what was *set* in that context and never falls
    back to the ContextVar's own default, so callbacks scheduled outside any
    host — the test driver's — have to be named explicitly.
    """
    host: str = handle.get_context().get(_current_host, DRIVER)
    return host


# What the loop remembers about one runnable callback: its seq, the label the
# trace records, the handle that runs it, and the callback itself — kept
# because that is where a task step says which task it belongs to, and a
# handle's callback is private to asyncio.
_ReadyEntry = tuple[int, str, asyncio.Handle, Callable[..., object]]


class _ReadyViews(Sequence[ReadyView]):
    """The ready queue as a policy sees it, named only where it looks.

    Naming the owner of a callback costs a lookup, and the two policies that
    ship with simloop — the seeded default and the scripted replay — decide
    on the length of the queue alone. So nothing is built up front: ``len``
    is the queue's length and only an entry that is actually indexed pays for
    its view. A policy that reads every entry, as a priority policy does,
    pays exactly what an eagerly built list would have cost.

    One instance serves the whole loop, over the live ready queue rather than
    a copy, so a step costs no allocation at all. The consequence is that the
    sequence describes the queue *now*: it is meant to be read inside
    ``choose`` and is not a snapshot to keep.

    Views are recomputed rather than cached, which is safe because computing
    one has no side effect — the owner numbers are handed out in
    ``create_task``, long before anything here looks at them — so indexing an
    entry twice simply returns equal tuples, the one caveat being that
    naming an owner reads ``__self__`` off the callback, which only a
    callable with a side-effecting ``__getattr__`` could notice.
    """

    __slots__ = ("_entries", "_owners")

    def __init__(
        self,
        entries: list[_ReadyEntry],
        owners: weakref.WeakKeyDictionary[asyncio.Task[Any], int],
    ) -> None:
        self._entries = entries
        self._owners = owners

    def __len__(self) -> int:
        return len(self._entries)

    @overload
    def __getitem__(self, index: int) -> ReadyView: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[ReadyView]: ...

    def __getitem__(self, index: int | slice) -> ReadyView | Sequence[ReadyView]:
        if isinstance(index, slice):
            return [self[position] for position in range(*index.indices(len(self)))]
        seq, label, _, callback = self._entries[index]
        # A task drives every one of its steps through ``call_soon``, and the
        # callback it passes is bound to the task, so ``__self__`` is where a
        # step says whose work it is. Anything no task owns is named by its
        # own seq, negated so the two kinds can never be confused and no two
        # unowned callbacks ever share a number.
        task = getattr(callback, "__self__", None)
        if isinstance(task, asyncio.Task):
            owner = self._owners.get(task)
            if owner is not None:
                return (owner, label)
        return (-1 - seq, label)


class SimLoop(asyncio.AbstractEventLoop):
    """An event loop where time is virtual and execution order is seeded.

    Callbacks never wait on wall-clock time: the clock advances only when the
    ready queue is empty, jumping straight to the next timer deadline. When
    several callbacks are ready at once, the next one to run is drawn from a
    seeded PRNG, so a given seed always reproduces the same execution order.

    Coroutine scheduling is inherited from the stdlib: ``asyncio.Task`` drives
    every step through ``call_soon``, so controlling ``call_soon`` dispatch is
    sufficient to control task interleaving. Anything this class does not
    implement (threads, signals, subprocesses) raises
    ``NotImplementedError`` from the base class — unsupported code fails
    loudly instead of silently breaking determinism.
    """

    def __init__(self, seed: int = 0) -> None:
        self._seed = seed
        self._policy: SchedulingPolicy = SeededPolicy(seed)
        # Every choice the policy made, in order, so a run's schedule can be
        # replayed (or edited and replayed) without its seed.
        self._choice_log: array[int] = array(CHOICE_TYPECODE)
        # Streams for user-facing entropy, derived from the seed but kept
        # separate from the scheduler's RNG: user draws must never perturb
        # scheduling order, and scheduling must never perturb user values.
        # String seeding hashes via SHA-512, so the streams are stable
        # across processes and interpreter versions.
        self._user_random = random.Random(f"{seed}:random")
        self._uuid_random = random.Random(f"{seed}:uuid")
        self._now = 0.0
        # Ready entries are (seq, label, handle, callback); seq is a global
        # creation counter that gives every scheduled callback a stable
        # identity.
        self._ready: list[_ReadyEntry] = []
        # Timer heap entries are (when, seq, label, handle, callback). seq
        # breaks ties between equal deadlines, so nothing after it — handles
        # and callbacks above all — is ever compared.
        self._timers: list[
            tuple[float, int, str, asyncio.TimerHandle, Callable[..., object]]
        ] = []
        self._next_seq = 0
        # Which task each of this loop's tasks is, as a number a policy can
        # compare: creation order, counted per loop so it is a property of
        # the run and not of the process. Weakly keyed because a long
        # campaign creates tasks by the million and this map must never be
        # the reason one of them stays alive.
        self._task_owners: weakref.WeakKeyDictionary[asyncio.Task[Any], int] = (
            weakref.WeakKeyDictionary()
        )
        self._next_owner = 0
        # Built once and reused: it reads the queue in place, so handing it
        # to the policy on every step costs nothing.
        self._ready_views = _ReadyViews(self._ready, self._task_owners)
        self._recorder = TraceRecorder()
        self._running = False
        self._closed = False
        self._stopping = False
        # Exceptions from callbacks and fire-and-forget tasks accumulate here
        # and are re-raised from run_until_complete once the loop stops.
        self._unhandled: list[BaseException] = []
        self._exception_handler: _ExceptionHandler | None = None
        self._task_factory: _TaskFactory | None = None
        # sock_connect(sock, addr) -> create_connection(sock=sock) is how
        # aiohttp reaches the network; the address is only visible in the
        # first call, so it is parked here until the upgrade claims it.
        self._sock_targets: dict[Any, tuple[Any, int]] = {}
        # The one thread the simulation lives in: the creating thread until a
        # run starts, the running thread from then on. call_soon_threadsafe
        # compares against it — from this thread the call is call_soon, from
        # any other it is a real concurrent thread and fences.
        self._thread_id = threading.get_ident()
        self._net = SimNetwork(self)

    @classmethod
    def _from_choices(cls, choices: Iterable[int], fault_seed: int) -> SimLoop:
        """Build a loop that replays ``choices`` on ``fault_seed``'s streams.

        The scheduler follows the recorded choices instead of drawing, while
        the network, user random and uuid streams remain the ones a plain
        ``SimLoop(fault_seed)`` would have: building that loop first and
        replacing only the policy is what guarantees it. A replay therefore
        perturbs the schedule and nothing else.

        Private on purpose: replaying a schedule apart from its seed is a
        debugging tool, and a recorded choice list is only meaningful against
        the exact code that produced it.
        """
        loop = cls(fault_seed)
        loop._policy = ScriptedPolicy(choices)
        return loop

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def seed(self) -> int:
        return self._seed

    @property
    def _choices(self) -> tuple[int, ...]:
        """Every scheduling choice made so far, oldest first.

        A snapshot rather than the live array: callers keep choice lists
        across runs and replay edited copies of them, and a snapshot cannot
        be invalidated by this loop running on.
        """
        return tuple(self._choice_log)

    @property
    def _diverged_at(self) -> int | None:
        """Step index where a replay first departed from its recording.

        ``None`` when the run followed its recording exactly, and always
        ``None`` for a seeded run, which has no recording to depart from.
        """
        return self._policy.diverged_at

    @property
    def trace(self) -> tuple[TraceEvent, ...]:
        return self._recorder.events

    def trace_hash(self) -> str:
        return self._recorder.hash()

    @property
    def net(self) -> SimNetwork:
        return self._net

    # ------------------------------------------------------------------
    # Clock and scheduling
    # ------------------------------------------------------------------

    def time(self) -> float:
        # What the calling task's machine believes the time is. Everything
        # internal — timer ordering, deadline advance, trace timestamps —
        # uses the true clock (self._now) directly, so skew never perturbs
        # scheduling and traces from skewed runs stay comparable.
        return self._now + self._net._offset_now()

    def _true_time(self) -> float:
        """The shared virtual clock, the same reading for whoever asks.

        The clock the simulation itself runs on, for the internals that must
        not see a host's skew — the network's trace timestamps above all.
        """
        return self._now

    def call_soon(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.Handle:
        self._check_closed()
        handle = asyncio.Handle(callback, args, self, context)
        seq = self._next_seq
        self._next_seq += 1
        label = _label(callback)
        self._ready.append((seq, label, handle, callback))
        # A schedule event names the host that *asked* for the callback, which
        # is not always the one that will run it: that difference is exactly
        # how a wakeup crossing machines shows up in the trace.
        self._recorder.record("schedule", self._now, seq, label, _current_host.get())
        return handle

    def call_soon_threadsafe(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.Handle:
        # From the simulation's own thread this is call_soon by definition —
        # libraries call it defensively without ever leaving the loop, and
        # nothing about the schedule changes. From any other thread the caller
        # is a real concurrent thread, whose timing no seed controls, so it
        # fences rather than smuggle a race into a deterministic run.
        if threading.get_ident() != self._thread_id:
            raise SimulationFenceError(
                "simloop does not simulate 'call_soon_threadsafe' from "
                "another thread: a real thread's timing is outside the "
                "simulation; see docs/supported-api.md for the supported "
                "asyncio subset"
            )
        return self.call_soon(callback, *args, context=context)

    def run_in_executor(
        self,
        executor: Any,
        func: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> asyncio.Future[Any]:
        """Run ``func`` inline at a scheduled step instead of on a thread.

        The submission becomes an ordinary ready-queue entry — labelled
        ``executor:<func>`` in the trace — so the seeded draw orders it
        against everything else and a run stays reproducible. The function
        executes synchronously when that step runs, costing no virtual time,
        and its result or exception lands on the returned future exactly as
        an executor worker would land it. ``asyncio.to_thread`` reaches the
        loop through this call, so it works under simulation too.

        Two honest consequences of running inline: the ``executor`` argument
        is never used — there is no pool, and nothing runs concurrently — and
        a function that blocks waiting for loop progress (joining a thread
        that needs a callback, waiting on a lock a coroutine holds) hangs the
        process rather than deadlocking detectably.
        """
        self._check_closed()
        if not callable(func):
            raise TypeError(f"a callable object is expected, got {func!r}")
        future: asyncio.Future[Any] = self.create_future()
        self.call_soon(_ExecutorJob(func, args, future))
        return future

    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        # A delay is a duration, so it is measured on the true clock and no
        # caller's offset touches it: a wrong wall clock does not make a
        # second take longer. Going through call_at instead would subtract
        # the caller's offset from a deadline that is already true.
        when = self._now + delay
        return self._call_at_true(
            when, when + self._net._offset_now(), callback, args, context
        )

    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        # ``when`` is a reading of the *scheduling* task's clock, so it is
        # converted to true time once, here, with that task's offset. The
        # callback runs later under the context asyncio copied into the
        # handle — the scheduling task's — so it reads the same clock the
        # scheduler did, which is coherent for the cases that matter:
        # asyncio.timeout, sleep and protocol timers all schedule from the
        # task they serve. Nothing re-converts per callback.
        return self._call_at_true(
            when - self._net._offset_now(), when, callback, args, context
        )

    def _call_at_true(
        self,
        when: float,
        shown: float,
        callback: Callable[[Unpack[_Ts]], object],
        args: tuple[Unpack[_Ts]],
        context: Context | None,
    ) -> asyncio.TimerHandle:
        """Push a timer whose ``when`` is already true time.

        The heap orders on true time — the one clock the loop itself runs on
        — while the handle advertises ``shown``, the same deadline on the
        clock the caller reads, so ``timer.when()`` stays comparable with
        that task's ``loop.time()``.
        """
        self._check_closed()
        timer = asyncio.TimerHandle(shown, callback, args, self, context)
        seq = self._next_seq
        self._next_seq += 1
        label = _label(callback)
        heapq.heappush(self._timers, (when, seq, label, timer, callback))
        self._recorder.record("schedule", self._now, seq, label, _current_host.get())
        return timer

    def _step(self) -> None:
        if not self._ready:
            self._advance_clock()
        # The one ordering decision in the whole loop, and the policy owns it:
        # every scheduling decision flows through this call, which is seeded
        # by default and scriptable for replay.
        #
        # This is the hottest call in the package, and the sequence handed
        # over is a standing view of the ready queue rather than a list built
        # here, so a policy that decides on the count alone — the default one
        # does — costs a step nothing at all.
        index = self._policy.choose(self._ready_views)
        assert index <= MAX_CHOICE, "ready queue outgrew the choice log"
        self._choice_log.append(index)
        seq, label, handle, _ = self._ready.pop(index)
        host = _host_of(handle)
        if handle.cancelled():
            # The draw itself is a scheduling decision, so a skipped handle
            # must appear in the trace for the replay proof to stay complete.
            self._recorder.record("cancel", self._now, seq, label, host)
            return
        self._recorder.record("run", self._now, seq, label, host)
        handle._run()

    def _advance_clock(self) -> None:
        while self._timers and self._timers[0][3].cancelled():
            _, seq, label, timer, _ = heapq.heappop(self._timers)
            self._recorder.record("cancel", self._now, seq, label, _host_of(timer))
        if not self._timers:
            raise SimulationDeadlockError(
                "nothing left to run: no ready callbacks and no pending timers"
            )
        self._now = max(self._now, self._timers[0][0])
        self._recorder.record("advance", self._now, -1, "")
        while self._timers and self._timers[0][0] <= self._now:
            _, seq, label, timer, callback = heapq.heappop(self._timers)
            if timer.cancelled():
                self._recorder.record("cancel", self._now, seq, label, _host_of(timer))
            else:
                self._ready.append((seq, label, timer, callback))

    # ------------------------------------------------------------------
    # Running
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        self._check_closed()
        if self._running:
            raise RuntimeError("this event loop is already running")
        self._running = True
        self._thread_id = threading.get_ident()
        events._set_running_loop(self)
        try:
            while not self._stopping and (self._ready or self._timers):
                self._step()
        finally:
            self._stopping = False
            self._running = False
            events._set_running_loop(None)

    def run_until_complete(self, future: Any) -> Any:
        fut = asyncio.ensure_future(future, loop=self)
        fut.add_done_callback(self._stop_when_done)
        try:
            self.run_forever()
        finally:
            fut.remove_done_callback(self._stop_when_done)
        completed = fut.done()
        if not completed:
            # Cancel the stalled task and step until it has processed the
            # cancellation, so it is never left pending for the garbage
            # collector to complain about. Draining stops as soon as no work
            # remains, keeping the seeded draw the only source of order.
            fut.cancel()
            # The drain steps outside run_forever, but a task will only step
            # while its loop is the running one (asyncio enforces this from
            # 3.14), so the drain must declare itself the same way.
            events._set_running_loop(self)
            try:
                while (self._ready or self._timers) and not fut.done():
                    self._step()
            finally:
                events._set_running_loop(None)
        # A fire-and-forget task that failed keeps itself alive through a
        # reference cycle (its exception's traceback pins the coroutine frame),
        # so its exception only reaches call_exception_handler when the cycle
        # collector finalizes it. Force that here, before the boundary check,
        # so an orphaned failure cannot slip past a run that otherwise looks
        # successful. This touches neither the clock nor the seeded draw.
        gc.collect()
        if not completed:
            # A collected failure explains the stall better than the generic
            # deadlock diagnosis, so it takes precedence here.
            if self._unhandled:
                raise self._unhandled[0]
            raise SimulationDeadlockError(
                "the awaited future never completed: all tasks are blocked"
            )
        # The awaited task's own outcome wins: its exception propagates as-is,
        # and only a normal return falls through to the orphaned failures.
        result = fut.result()
        if self._unhandled:
            raise self._unhandled[0]
        return result

    def _stop_when_done(self, fut: asyncio.Future[Any]) -> None:
        self.stop()

    def stop(self) -> None:
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._running:
            raise RuntimeError("cannot close a running event loop")
        self._sock_targets.clear()
        # Connections still open at the end of a run — what a pooling client
        # leaves behind — hold their liveness descriptors inside the socket
        # object's reference cycle with the transport, so the descriptors
        # would only come back when the cycle collector ran. Nothing can poll
        # them once the loop is closed, so release them here instead.
        for transport in self._net._streams.values():
            if transport._extra_socket is not None:
                transport._extra_socket._dispose()
        self._closed = True

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("event loop is closed")

    # ------------------------------------------------------------------
    # Task and future factories
    # ------------------------------------------------------------------

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Any,
        *,
        name: str | None = None,
        context: Context | None = None,
        eager_start: bool | None = None,
    ) -> asyncio.Task[Any]:
        self._check_closed()
        if eager_start:
            # An eager first step runs at creation time, before the ready
            # queue ever sees the task, so the seeded draw would never get to
            # order it against anything.
            _fence("create_task(eager_start=True)")
        if self._task_factory is None:
            task: asyncio.Task[Any] = asyncio.Task(
                coro, loop=self, name=name, context=context
            )
        else:
            factory: Any = self._task_factory
            task = (
                factory(self, coro)
                if context is None
                else factory(self, coro, context=context)
            )
            if name is not None:
                set_name = getattr(task, "set_name", None)
                if set_name is not None:
                    set_name(name)
        # The task queued its own first step from its constructor, but no
        # policy can have looked at the ready queue since — the loop is not
        # running here — so numbering it now still names that first step.
        self._task_owners[task] = self._next_owner
        self._next_owner += 1
        self._net._register_task(task)
        return task

    def _task_owner(self, task: asyncio.Task[Any]) -> int:
        """Which task this is, counted in creation order on this loop.

        The number a policy sees in the ready view of every step ``task``
        takes. Only tasks this loop created have one: a bare
        ``asyncio.Task(coro)`` never passes through here, and its steps are
        named individually instead, as any other unowned callback is.
        """
        return self._task_owners[task]

    async def create_datagram_endpoint(
        self,
        protocol_factory: Any,
        local_addr: Any = None,
        remote_addr: Any = None,
        **kwargs: Any,
    ) -> Any:
        _reject_kwargs("create_datagram_endpoint", kwargs)
        return await self._net._open_datagram_endpoint(
            protocol_factory, local_addr, remote_addr
        )

    async def create_connection(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        **kwargs: Any,
    ) -> Any:
        sock = kwargs.pop("sock", None)
        _reject_kwargs("create_connection", kwargs)
        if sock is not None:
            # The stdlib treats a passed-in socket as already connected and
            # takes ownership of it. Here "connected" means sock_connect
            # parked a target for it; the real descriptor is closed at once
            # because the simulation only needed the address it carried.
            if host is not None or port is not None:
                raise ValueError(
                    "host/port and sock can not be specified at the same time"
                )
            target = self._sock_targets.pop(sock, None)
            if target is None:
                raise OSError(
                    "the given socket was not connected via sock_connect on this loop"
                )
            sock.close()
            host, port = target
        return await self._net._open_connection(protocol_factory, host, port)

    async def create_server(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        **kwargs: Any,
    ) -> Any:
        kwargs.pop("backlog", None)  # accepted and irrelevant: no accept queue
        _reject_kwargs("create_server", kwargs)
        return await self._net._start_server(protocol_factory, host, port)

    async def getaddrinfo(
        self,
        host: Any,
        port: Any,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        """Resolve a name against the network's host table instead of DNS.

        Resolution is a table lookup, not a scheduling decision: it never
        blocks, never reaches the real resolver, and records no trace event,
        so a run's schedule is unaffected by how often names are resolved.
        """
        return self._net._getaddrinfo(host, port, family, type, proto, flags)

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> tuple[str, str]:
        return self._net._getnameinfo(sockaddr, flags)

    def set_task_factory(self, factory: _TaskFactory | None) -> None:
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self) -> _TaskFactory | None:
        return self._task_factory

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def set_exception_handler(self, handler: _ExceptionHandler | None) -> None:
        if handler is not None and not callable(handler):
            raise TypeError(
                f"a callable object or None is expected, got {handler!r}"
            )
        self._exception_handler = handler

    def get_exception_handler(self) -> _ExceptionHandler | None:
        return self._exception_handler

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is None:
            self.default_exception_handler(context)
            return
        try:
            self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as handler_error:
            # A broken exception handler is itself a failure that must not
            # vanish: surface it, and fall back to the default policy for
            # the original context.
            self._unhandled.append(handler_error)
            self.default_exception_handler(context)

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        # A simulation must not swallow errors. Collect real failures so that
        # run_until_complete re-raises them once the loop stops. This covers
        # fire-and-forget tasks, whose exceptions otherwise reach here only
        # from Task.__del__ at GC time, where a raise would be unraisable and
        # the run would falsely report success. Message-only contexts (e.g. a
        # still-pending task being destroyed) are informational, not failures:
        # they go to stderr and must never abort an otherwise successful run.
        exc = context.get("exception")
        if isinstance(exc, BaseException):
            self._unhandled.append(exc)
        else:
            print(
                "simloop:", context.get("message", "unhandled error"), file=sys.stderr
            )

    def get_debug(self) -> bool:
        return False

    def set_debug(self, enabled: bool) -> None:
        pass

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        pass

    # ------------------------------------------------------------------
    # Unsupported surface
    # ------------------------------------------------------------------
    #
    # Subprocesses, signals, file descriptors and real threads all reach
    # outside the simulation, so they cannot participate in a deterministic
    # virtual-time run. Each one fails loudly with NotImplementedError
    # instead of quietly breaking reproducibility.
    #
    # These are declared explicitly rather than inherited because the base
    # class marks them abstract: the signatures mirror the stubs (reproducing
    # the callback/args type variable where one is present) so a subclass
    # remains a well-typed AbstractEventLoop.

    def add_reader(
        self,
        fd: Any,
        callback: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> None:
        _fence("add_reader")

    def add_writer(
        self,
        fd: Any,
        callback: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> None:
        _fence("add_writer")

    def add_signal_handler(
        self,
        sig: Any,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
    ) -> None:
        _fence("add_signal_handler")

    def set_default_executor(self, *args: Any, **kwargs: Any) -> Any:
        _fence("set_default_executor")

    def shutdown_asyncgens(self, *args: Any, **kwargs: Any) -> Any:
        _fence("shutdown_asyncgens")

    def shutdown_default_executor(self, *args: Any, **kwargs: Any) -> Any:
        _fence("shutdown_default_executor")

    def start_tls(self, *args: Any, **kwargs: Any) -> Any:
        _fence("start_tls")

    def sendfile(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sendfile")

    def sock_sendfile(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_sendfile")

    def connect_read_pipe(self, *args: Any, **kwargs: Any) -> Any:
        _fence("connect_read_pipe")

    def connect_write_pipe(self, *args: Any, **kwargs: Any) -> Any:
        _fence("connect_write_pipe")

    def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
        _fence("subprocess_shell")

    def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
        _fence("subprocess_exec")

    def remove_reader(self, *args: Any, **kwargs: Any) -> Any:
        _fence("remove_reader")

    def remove_writer(self, *args: Any, **kwargs: Any) -> Any:
        _fence("remove_writer")

    def remove_signal_handler(self, *args: Any, **kwargs: Any) -> Any:
        _fence("remove_signal_handler")

    def sock_recv(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_recv")

    def sock_recv_into(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_recv_into")

    def sock_sendall(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_sendall")

    async def sock_connect(self, sock: Any, address: Any) -> None:
        # aiohttp's connector creates a real TCP socket and connects it here
        # before handing it to create_connection(sock=...). The simulation
        # accepts exactly that shape; every other socket kind still fences.
        if (
            getattr(sock, "family", None) != socket.AF_INET
            or getattr(sock, "type", None) != socket.SOCK_STREAM
        ):
            raise SimulationFenceError(
                "simloop does not simulate 'sock_connect' for anything but "
                "AF_INET stream sockets; see docs/supported-api.md for the "
                "supported asyncio subset"
            )
        if (
            isinstance(address, (str, bytes, bytearray))
            or not isinstance(address, Sequence)
            or len(address) != 2
            or not isinstance(address[0], (str, bytes))
            or isinstance(address[1], bool)
            or not isinstance(address[1], int)
        ):
            # Caught here rather than deeper: a bare host string would be read
            # character by character, and a string port would surface much
            # later as a confusing error about something else entirely.
            raise OSError(
                f"sock_connect needs an AF_INET (host, port) address, got {address!r}"
            )
        host, port = address[0], address[1]
        self._net._resolve(host)  # unknown targets fail here, loudly
        # No packet moves and no time passes yet: the connection handshake
        # (and its one-RTT cost) happens when create_connection claims the
        # socket, keeping the total cost identical to a direct connect.
        self._sock_targets[sock] = (host, port)

    def sock_accept(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_accept")

    def sock_sendto(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_sendto")

    def sock_recvfrom(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_recvfrom")

    def sock_recvfrom_into(self, *args: Any, **kwargs: Any) -> Any:
        _fence("sock_recvfrom_into")
