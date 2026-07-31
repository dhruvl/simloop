"""Shared plumbing for the compatibility probes.

A probe drives one library's happy path under a SimLoop and returns a short
description of what it managed to exercise. This module owns everything the
probes should not repeat: loop setup, a bounded virtual-time budget so a
library that waits forever ends the run instead of hanging it, and the single
verdict vocabulary the table in docs/compatibility.md is built from.

A verdict describes one run and nothing more: ``works`` means the probe's own
description of what it did came back, not that the library is supported.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from simloop import SimLoop, SimulationFenceError

Probe = Callable[[SimLoop], Awaitable[str]]

# Virtual seconds, so the ceiling costs no wall-clock time. Generous enough
# that a library's own timeouts (aiohttp's keep-alive, websockets' close
# handshake) fire first and are reported as themselves.
BUDGET = 60.0


class BudgetExceeded(Exception):
    """The probe was still running when its virtual-time budget ran out."""


def _first_fence(error: BaseException) -> SimulationFenceError | None:
    """Find the fence inside whatever the library wrapped it in.

    Task groups raise ``ExceptionGroup``s and client libraries re-raise
    through ``__cause__``, so the fence is rarely the outermost exception.
    """
    if isinstance(error, SimulationFenceError):
        return error
    if isinstance(error, BaseExceptionGroup):
        for nested in error.exceptions:
            found = _first_fence(nested)
            if found is not None:
                return found
    inner = error.__cause__ or error.__context__
    return _first_fence(inner) if inner is not None else None


def _describe(error: BaseException) -> str:
    message = str(error)
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


def run(probe: Probe, *, seed: int = 0, budget: float = BUDGET) -> str:
    """Run one probe and report its outcome as a single verdict line."""
    loop = SimLoop(seed=seed)

    async def bounded() -> str:
        limit = asyncio.timeout(budget)
        try:
            async with limit:
                return await probe(loop)
        except TimeoutError as expiry:
            if limit.expired():
                raise BudgetExceeded(f"still running after {budget}s") from expiry
            raise

    try:
        return f"works: {loop.run_until_complete(bounded())}"
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        # Catching broadly is the point: whatever the library did to the run
        # is the finding, including a cancellation that escaped it.
        fence = _first_fence(error)
        return f"fenced: {fence}" if fence else f"fails: {_describe(error)}"
    finally:
        loop.close()
