"""A deterministic asyncio event loop with a virtual clock and seeded scheduling."""

from __future__ import annotations

import asyncio
import gc
import heapq
import random
import sys
from asyncio import events
from collections.abc import Callable
from contextvars import Context
from typing import Any, TypeVarTuple, Unpack

from simloop._trace import TraceEvent, TraceRecorder

_Ts = TypeVarTuple("_Ts")


class SimulationDeadlockError(RuntimeError):
    """No runnable callbacks or timers remain, but the awaited future is not done.

    This usually means a lost wakeup: some task is waiting on a future or queue
    that nothing will ever complete.
    """


def _label(callback: Callable[..., object]) -> str:
    # Labels feed the trace hash, so they must be stable across processes:
    # qualified names only, never repr() (which can embed memory addresses).
    name = getattr(callback, "__qualname__", None)
    if isinstance(name, str):
        return name
    return type(callback).__name__


class SimLoop(asyncio.AbstractEventLoop):
    """An event loop where time is virtual and execution order is seeded.

    Callbacks never wait on wall-clock time: the clock advances only when the
    ready queue is empty, jumping straight to the next timer deadline. When
    several callbacks are ready at once, the next one to run is drawn from a
    seeded PRNG, so a given seed always reproduces the same execution order.

    Coroutine scheduling is inherited from the stdlib: ``asyncio.Task`` drives
    every step through ``call_soon``, so controlling ``call_soon`` dispatch is
    sufficient to control task interleaving. Anything this class does not
    implement (networking, executors, signals, threads) raises
    ``NotImplementedError`` from the base class — unsupported code fails
    loudly instead of silently breaking determinism.
    """

    def __init__(self, seed: int = 0) -> None:
        self._seed = seed
        self._rng = random.Random(seed)
        self._now = 0.0
        # Ready entries are (seq, label, handle); seq is a global creation
        # counter that gives every scheduled callback a stable identity.
        self._ready: list[tuple[int, str, asyncio.Handle]] = []
        # Timer heap entries are (when, seq, label, handle). seq breaks ties
        # between equal deadlines, so handles themselves are never compared.
        self._timers: list[tuple[float, int, str, asyncio.TimerHandle]] = []
        self._next_seq = 0
        self._recorder = TraceRecorder()
        self._running = False
        self._closed = False
        self._stopping = False
        # Exceptions from callbacks and fire-and-forget tasks accumulate here
        # and are re-raised from run_until_complete once the loop stops.
        self._unhandled: list[BaseException] = []

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def seed(self) -> int:
        return self._seed

    @property
    def trace(self) -> tuple[TraceEvent, ...]:
        return self._recorder.events

    def trace_hash(self) -> str:
        return self._recorder.hash()

    # ------------------------------------------------------------------
    # Clock and scheduling
    # ------------------------------------------------------------------

    def time(self) -> float:
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
        self._ready.append((seq, label, handle))
        self._recorder.record("schedule", self._now, seq, label)
        return handle

    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        return self.call_at(self._now + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        self._check_closed()
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        seq = self._next_seq
        self._next_seq += 1
        label = _label(callback)
        heapq.heappush(self._timers, (when, seq, label, timer))
        self._recorder.record("schedule", self._now, seq, label)
        return timer

    def _step(self) -> None:
        if not self._ready:
            self._advance_clock()
        # The one nondeterminism source in the whole loop, and it is seeded:
        # every scheduling decision flows through this draw.
        index = self._rng.randrange(len(self._ready))
        seq, label, handle = self._ready.pop(index)
        if handle.cancelled():
            return
        self._recorder.record("run", self._now, seq, label)
        handle._run()

    def _advance_clock(self) -> None:
        while self._timers and self._timers[0][3].cancelled():
            heapq.heappop(self._timers)
        if not self._timers:
            raise SimulationDeadlockError(
                "nothing left to run: no ready callbacks and no pending timers"
            )
        self._now = max(self._now, self._timers[0][0])
        self._recorder.record("advance", self._now, -1, "")
        while self._timers and self._timers[0][0] <= self._now:
            _, seq, label, timer = heapq.heappop(self._timers)
            if not timer.cancelled():
                self._ready.append((seq, label, timer))

    # ------------------------------------------------------------------
    # Running
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        self._check_closed()
        if self._running:
            raise RuntimeError("this event loop is already running")
        self._running = True
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
            while (self._ready or self._timers) and not fut.done():
                self._step()
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
    ) -> asyncio.Task[Any]:
        self._check_closed()
        return asyncio.Task(coro, loop=self, name=name, context=context)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def call_exception_handler(self, context: dict[str, Any]) -> None:
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

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        self.call_exception_handler(context)

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
    # Networking, executors, subprocesses, signals, file descriptors and
    # thread-safe scheduling all reach outside the simulation, so they cannot
    # participate in a deterministic virtual-time run. Each one fails loudly
    # with NotImplementedError instead of quietly breaking reproducibility.
    #
    # These are declared explicitly rather than inherited because the base
    # class marks them abstract: the signatures mirror the stubs (reproducing
    # the callback/args type variable where one is present) so a subclass
    # remains a well-typed AbstractEventLoop.

    def call_soon_threadsafe(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.Handle:
        raise NotImplementedError("call_soon_threadsafe is not supported")

    def run_in_executor(
        self,
        executor: Any,
        func: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> Any:
        raise NotImplementedError("run_in_executor is not supported")

    def add_reader(
        self,
        fd: Any,
        callback: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> None:
        raise NotImplementedError("add_reader is not supported")

    def add_writer(
        self,
        fd: Any,
        callback: Callable[[Unpack[_Ts]], Any],
        *args: Unpack[_Ts],
    ) -> None:
        raise NotImplementedError("add_writer is not supported")

    def add_signal_handler(
        self,
        sig: Any,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
    ) -> None:
        raise NotImplementedError("add_signal_handler is not supported")

    def set_default_executor(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("set_default_executor is not supported")

    def set_task_factory(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("set_task_factory is not supported")

    def get_task_factory(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("get_task_factory is not supported")

    def set_exception_handler(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("set_exception_handler is not supported")

    def get_exception_handler(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("get_exception_handler is not supported")

    def shutdown_asyncgens(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("shutdown_asyncgens is not supported")

    def shutdown_default_executor(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("shutdown_default_executor is not supported")

    def getaddrinfo(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("getaddrinfo is not supported")

    def getnameinfo(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("getnameinfo is not supported")

    def create_connection(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("create_connection is not supported")

    def create_server(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("create_server is not supported")

    def start_tls(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("start_tls is not supported")

    def sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sendfile is not supported")

    def sock_sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_sendfile is not supported")

    def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("create_datagram_endpoint is not supported")

    def connect_read_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("connect_read_pipe is not supported")

    def connect_write_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("connect_write_pipe is not supported")

    def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("subprocess_shell is not supported")

    def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("subprocess_exec is not supported")

    def remove_reader(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("remove_reader is not supported")

    def remove_writer(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("remove_writer is not supported")

    def remove_signal_handler(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("remove_signal_handler is not supported")

    def sock_recv(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_recv is not supported")

    def sock_recv_into(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_recv_into is not supported")

    def sock_sendall(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_sendall is not supported")

    def sock_connect(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_connect is not supported")

    def sock_accept(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_accept is not supported")

    def sock_sendto(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_sendto is not supported")

    def sock_recvfrom(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_recvfrom is not supported")

    def sock_recvfrom_into(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("sock_recvfrom_into is not supported")
