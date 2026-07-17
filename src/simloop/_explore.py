"""Run a simulation test under many seeds and report the first failure.

This module is plain library code: it never imports pytest. The pytest
integration in ``_pytest_plugin`` feeds session options in through the
module-level ``overrides`` object, which keeps ``import simloop`` free of
any test-framework dependency.
"""

from __future__ import annotations

import asyncio
import functools
import os
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any, overload

from simloop._loop import SimLoop
from simloop._trace import TraceEvent


@dataclass(frozen=True, slots=True)
class PendingTask:
    """One task still pending when a seed failed."""

    host: str
    name: str
    awaiting: str
    where: str


@dataclass(frozen=True, slots=True)
class SeedReport:
    """Everything known about the first failing seed."""

    seed: int
    seeds_passed: int
    exception: Exception
    trace_events: tuple[TraceEvent, ...]
    trace_hash: str
    pending: tuple[PendingTask, ...]

    def render(self, test_id: str | None = None) -> str:
        lines = [
            f"simloop: failed at seed {self.seed} "
            f"({self.seeds_passed} seeds passed first)"
        ]
        if test_id is not None:
            lines.append(
                f"replay: pytest '{test_id}' --simloop-replay={self.seed}"
            )
        if self.trace_events:
            lines.append("")
            lines.append(f"last {len(self.trace_events)} trace events:")
            for event in self.trace_events:
                lines.append(
                    f"  [t={event.when:.4f}] {event.kind:<8} "
                    f"seq={event.seq}  {event.label}"
                )
        if self.pending:
            lines.append("pending tasks by host:")
            for task in self.pending:
                lines.append(
                    f"  {task.host}  Task {task.name!r}  "
                    f"awaiting {task.awaiting}  at {task.where}"
                )
        return "\n".join(lines)


def explore(
    fn: Callable[[], Coroutine[Any, Any, object]],
    seeds: Iterable[int],
    *,
    trace_tail: int = 20,
) -> SeedReport | None:
    """Run ``fn`` once per seed on a fresh SimLoop; stop at the first failure.

    Returns a :class:`SeedReport` for the first seed whose run raised an
    ``Exception``, or ``None`` when every seed passed. ``BaseException``s
    that are not test failures (``KeyboardInterrupt``, ``SystemExit``)
    propagate immediately.
    """
    passed = 0
    for seed in seeds:
        loop = SimLoop(seed)
        try:
            try:
                loop.run_until_complete(fn())
            except Exception as exc:
                return SeedReport(
                    seed=seed,
                    seeds_passed=passed,
                    exception=exc,
                    trace_events=loop.trace[-trace_tail:] if trace_tail else (),
                    trace_hash=loop.trace_hash(),
                    pending=_pending_tasks(loop),
                )
            finally:
                _drain(loop)
        finally:
            loop.close()
        passed += 1
    return None


def _pending_tasks(loop: SimLoop) -> tuple[PendingTask, ...]:
    found: list[PendingTask] = []
    for host, tasks in loop.net._tasks.items():
        for task in tasks:
            if task.done():
                continue
            awaiting = "?"
            where = "?"
            stack = task.get_stack()
            if stack:
                frame = stack[-1]
                awaiting = frame.f_code.co_name
                where = f"{_short_path(frame.f_code.co_filename)}:{frame.f_lineno}"
            found.append(
                PendingTask(
                    host=host, name=task.get_name(), awaiting=awaiting, where=where
                )
            )
    return tuple(found)


def _short_path(filename: str) -> str:
    cwd = os.getcwd()
    if filename.startswith(cwd + os.sep):
        return filename[len(cwd) + 1 :]
    return filename


def _drain(loop: SimLoop) -> None:
    """Cancel tasks a finished run left pending and let them unwind.

    Without this, an abandoned task's garbage collection would route
    "Task was destroyed but it is pending!" through the loop's exception
    handler onto stderr long after the run ended.
    """
    pending = [
        task
        for tasks in loop.net._tasks.values()
        for task in tasks
        if not task.done()
    ]
    if not pending:
        return
    for task in pending:
        task.cancel()
    try:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    except Exception:
        # The run is already over; teardown failures add nothing.
        pass


@dataclass
class _Overrides:
    """Session state the pytest plugin writes; consulted by sim_test wrappers.

    ``seeds`` and ``replay`` mirror the --simloop-* options; ``node_id`` is
    the test currently running, so reports can print an exact replay
    command. The counters feed the plugin's terminal summary.
    """

    seeds: int | None = None
    replay: int | None = None
    node_id: str | None = None
    sim_tests: int = 0
    seeds_explored: int = 0


overrides = _Overrides()

_TestFn = Callable[..., Coroutine[Any, Any, object]]


@overload
def sim_test(fn: _TestFn, /) -> Callable[..., None]: ...


@overload
def sim_test(
    *, seeds: int = ..., trace_tail: int = ...
) -> Callable[[_TestFn], Callable[..., None]]: ...


def sim_test(
    fn: _TestFn | None = None,
    /,
    *,
    seeds: int = 10,
    trace_tail: int = 20,
) -> Callable[..., None] | Callable[[_TestFn], Callable[..., None]]:
    """Turn an ``async def`` test into a seed-exploring synchronous test.

    The wrapper runs the coroutine under ``seeds`` seeds (0..N-1) via
    :func:`explore` and re-raises the first failure with the rendered
    report attached as an exception note. Under pytest, the --simloop-seeds
    and --simloop-replay options override the decorator's arguments.
    """
    if seeds < 1:
        raise ValueError("seeds must be at least 1")

    def decorate(test_fn: _TestFn) -> Callable[..., None]:
        @functools.wraps(test_fn)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            if overrides.replay is not None:
                seed_set: range | tuple[int, ...] = (overrides.replay,)
            elif overrides.seeds is not None:
                seed_set = range(overrides.seeds)
            else:
                seed_set = range(seeds)
            if len(seed_set) < 1:
                raise ValueError("seeds must be at least 1")
            report = explore(
                functools.partial(test_fn, *args, **kwargs),
                seed_set,
                trace_tail=trace_tail,
            )
            overrides.sim_tests += 1
            if report is None:
                overrides.seeds_explored += len(seed_set)
                return
            overrides.seeds_explored += report.seeds_passed + 1
            report.exception.add_note(report.render(overrides.node_id))
            raise report.exception

        return wrapper

    if fn is not None:
        return decorate(fn)
    return decorate
