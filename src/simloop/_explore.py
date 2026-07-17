"""Run a simulation test under many seeds and report the first failure.

This module is plain library code: it never imports pytest. The pytest
integration in ``_pytest_plugin`` feeds session options in through the
module-level ``overrides`` object, which keeps ``import simloop`` free of
any test-framework dependency.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any

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
                where = f"{frame.f_code.co_filename}:{frame.f_lineno}"
            found.append(
                PendingTask(
                    host=host, name=task.get_name(), awaiting=awaiting, where=where
                )
            )
    return tuple(found)


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
