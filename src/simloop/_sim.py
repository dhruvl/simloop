"""Seeded stand-ins for common sources of nondeterminism in user code.

Inside a running SimLoop, ``sim.random``, ``sim.uuid4`` and ``sim.time`` draw
from streams derived from the loop's seed, so their values replay exactly.
Outside a simulation they fall back to the stdlib, so code written against
them behaves normally in production.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid

from simloop._loop import SimLoop

_fallback_random = random.Random()


def _running_sim_loop() -> SimLoop | None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    return loop if isinstance(loop, SimLoop) else None


class Sim:
    """Facade over the running SimLoop's user-facing entropy and clock."""

    @property
    def random(self) -> random.Random:
        """A ``random.Random``: seeded per loop inside a run, real outside."""
        loop = _running_sim_loop()
        if loop is None:
            return _fallback_random
        return loop._user_random

    def uuid4(self) -> uuid.UUID:
        """A version-4 UUID: seed-derived inside a run, real outside."""
        loop = _running_sim_loop()
        if loop is None:
            return uuid.uuid4()
        return uuid.UUID(int=loop._uuid_random.getrandbits(128), version=4)

    def time(self) -> float:
        """Seconds: virtual loop time inside a run (starting at 0.0, not the
        epoch), wall-clock ``time.time()`` outside."""
        loop = _running_sim_loop()
        if loop is None:
            return time.time()
        return loop.time()


sim = Sim()
