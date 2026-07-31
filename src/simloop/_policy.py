"""Scheduling policies: how the loop picks the next ready callback.

Every ordering decision the loop makes flows through a policy, so swapping
the policy swaps the schedule and nothing else — the clock, the network fault
stream and the user-facing entropy streams stay exactly where the seed put
them. That separation is what lets a recorded run be replayed, or edited and
replayed, without touching its seed.
"""

from __future__ import annotations

import random
from array import array
from collections.abc import Iterable
from typing import Protocol

# One choice per step adds up over a long run, so choice lists are stored as
# a machine-word array instead of boxed ints. "L" is unsigned long, whose
# width is platform-dependent, so the accepted range is measured rather than
# assumed.
CHOICE_TYPECODE = "L"
MAX_CHOICE = 2 ** (8 * array(CHOICE_TYPECODE).itemsize) - 1


class SchedulingPolicy(Protocol):
    """Chooses which of ``ready`` runnable callbacks runs next."""

    diverged_at: int | None

    def choose(self, ready: int) -> int: ...


class SeededPolicy:
    """The default policy: one seeded draw over the ready queue per step.

    Built from the seed value the loop was given and drawing with
    ``randrange``, so the sequence of draws — and therefore the trace hash —
    is what the loop produced when it owned the PRNG itself.
    """

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self.diverged_at: int | None = None  # a seeded run has nothing to diverge from

    def choose(self, ready: int) -> int:
        return self._rng.randrange(ready)


class ScriptedPolicy:
    """Replays a recorded sequence of scheduler choices.

    Drift is tolerated rather than fatal. A choice list edited by hand, or
    replayed against a run that took a different turn, can name an index the
    ready queue no longer has: that clamps to the last ready callback, and a
    run that outlives its recording falls back to FIFO. Either way the run
    finishes and can be judged by its outcome, with ``diverged_at`` holding
    the first step where the replay stopped being faithful. Only input that
    could never have been recorded is an error, and it fails at construction.
    """

    def __init__(self, choices: Iterable[int]) -> None:
        recorded = tuple(choices)
        for choice in recorded:
            if choice < 0:
                raise ValueError(f"scheduler choices must be non-negative: {choice}")
            if choice > MAX_CHOICE:
                raise ValueError(f"scheduler choice is too large: {choice}")
        self._choices = array(CHOICE_TYPECODE, recorded)
        self._step = 0
        self.diverged_at: int | None = None

    def choose(self, ready: int) -> int:
        step = self._step
        self._step += 1
        if step >= len(self._choices):
            self._diverge(step)
            return 0
        choice = self._choices[step]
        if choice >= ready:
            self._diverge(step)
            return ready - 1
        return choice

    def _diverge(self, step: int) -> None:
        # Only the first departure is worth reporting: everything after it
        # replays a schedule the recording never described anyway.
        if self.diverged_at is None:
            self.diverged_at = step
