"""One run of a workload on one loop, from start to teardown.

The seed explorer and the schedule shrinker both need the same sequence —
run the coroutine, cancel whatever it abandoned, close the loop — and both
depend on the same ordering constraint: a run's trace, choice log and pending
tasks have to be read before teardown, because teardown schedules callbacks
of its own and would otherwise appear in them. Keeping the sequence here is
what keeps that constraint in one place instead of two.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from simloop._loop import SimLoop

# A workload is called once per run: it returns a fresh coroutine each time,
# since a coroutine can only be awaited once.
Workload = Callable[[], Coroutine[Any, Any, object]]


def run_once(loop: SimLoop, fn: Workload) -> Exception | None:
    """Run ``fn()`` to completion on ``loop`` and report how it ended.

    Returns the ``Exception`` that ended the run, or ``None`` when it
    finished. ``BaseException``s that are not test failures
    (``KeyboardInterrupt``, ``SystemExit``) propagate.

    The loop is left open and undrained: read whatever the run produced from
    it, then call :func:`finish`.
    """
    try:
        loop.run_until_complete(fn())
    except Exception as exc:
        return exc
    return None


def finish(loop: SimLoop) -> None:
    """Tear down what a finished run abandoned, then close the loop."""
    _drain(loop)
    loop.close()


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
