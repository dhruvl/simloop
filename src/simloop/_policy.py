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
from collections.abc import Iterable, Sequence
from typing import Protocol

# One choice per step adds up over a long run, so choice lists are stored as
# a machine-word array instead of boxed ints. "L" is unsigned long, whose
# width is platform-dependent, so the accepted range is measured rather than
# assumed.
CHOICE_TYPECODE = "L"
MAX_CHOICE = 2 ** (8 * array(CHOICE_TYPECODE).itemsize) - 1

ReadyView = tuple[int, str]
"""What a policy is told about one runnable callback: ``(owner, label)``.

``owner`` names who the callback belongs to. Every step of a task carries
that task's creation index on this loop, a non-negative number that is the
same at every step the task takes, so a policy can tell "this task again"
from "a different task". Anything else — a timer, a protocol callback, a
plain function — gets a negative number unique to it, never equal to
another entry's and never mistakable for a task. Ownership is read off the
callback, so a callback that is a bound method of a live task is that
task's work whoever queued it; only callbacks with no task behind them
land on the negative side.

``label`` is the same qualified callback name the trace records.

Deliberately a plain tuple and not the handle: a policy decides order, and
handing it something it could *run* would make that a matter of trust rather
than of construction.
"""


class SchedulingPolicy(Protocol):
    """Chooses which of the ``ready`` runnable callbacks runs next.

    The return value indexes ``ready``. Policies are free to ignore the views
    and decide on the count alone — the two shipped here do.
    """

    diverged_at: int | None

    def choose(self, ready: Sequence[ReadyView]) -> int: ...


class SeededPolicy:
    """The default policy: one seeded draw over the ready queue per step.

    Built from the seed value the loop was given and drawing with
    ``randrange``, so the sequence of draws — and therefore the trace hash —
    is what the loop produced when it owned the PRNG itself. The ready views
    are deliberately unread: a uniform draw over the queue is defined by its
    length, and consulting anything else would move every recorded trace.
    """

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self.diverged_at: int | None = None  # a seeded run has nothing to diverge from

    def choose(self, ready: Sequence[ReadyView]) -> int:
        return self._rng.randrange(len(ready))


class ScriptedPolicy:
    """Replays a recorded sequence of scheduler choices.

    Only the length of the ready queue is read, never the views: a recording
    is a list of indices, and an index means what it meant when it was
    recorded only if nothing else enters the decision.

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

    def choose(self, ready: Sequence[ReadyView]) -> int:
        step = self._step
        self._step += 1
        if step >= len(self._choices):
            self._diverge(step)
            return 0
        choice = self._choices[step]
        if choice >= len(ready):
            self._diverge(step)
            return len(ready) - 1
        return choice

    def _diverge(self, step: int) -> None:
        # Only the first departure is worth reporting: everything after it
        # replays a schedule the recording never described anyway.
        if self.diverged_at is None:
            self.diverged_at = step


class PCTPolicy:
    """Priority-based exploration with provable bug-finding odds.

    Burckhardt et al.'s PCT (ASPLOS 2010): give every chain of work a
    distinct random priority, always run the highest-priority ready entry,
    and lower the leader's priority at ``depth - 1`` random steps. Any bug
    needing ``depth`` ordered scheduling constraints is hit with probability
    at least 1/(n * horizon**(depth-1)) per run — a guarantee uniform random
    draws cannot make. Owners unseen so far draw a fresh priority on first
    sight, the standard adaptation for tasks that are created as the run
    goes. A callback no task owns carries an id unique to that callback, so
    each such entry is a chain of its own that draws once and runs once;
    that is what PCT does with a one-shot event, not an accident of the
    numbering. It does mean a change point that lands on a one-shot winner
    spends a demotion level on a chain with no future — demoting something
    that was never going to run again changes nothing — so read the depth-d
    bound as a statement about schedules where owned chains do the deciding,
    and expect it to be optimistic where one-shot callbacks dominate the
    ready queue.

    ``horizon`` is a guess at how many steps the run will take, and the odds
    are only as good as the guess. Change points are sampled without
    replacement from ``range(horizon)``: a run that ends early passes only
    some of them and gets fewer than ``depth - 1`` demotions, and a run that
    overruns spends all of them in its first ``horizon`` steps, after which
    no change point can fall — the tail runs at fixed priorities and the
    bound above says nothing about bugs that live there. Both schedules are
    legal; neither is the one the guarantee describes.
    """

    def __init__(self, seed: int, depth: int = 3, horizon: int = 10_000) -> None:
        if depth < 1:
            raise ValueError(f"depth must be >= 1, got {depth}")
        if horizon < 1:
            raise ValueError(f"horizon must be >= 1, got {horizon}")
        if depth - 1 > horizon:
            raise ValueError(
                f"horizon must leave room for depth - 1 change points: "
                f"depth {depth} needs horizon >= {depth - 1}, got {horizon}"
            )
        # Seeded off a string rather than the bare number so a PCT run and a
        # seeded run of the same seed do not walk the same stream of draws.
        self._rng = random.Random(f"{seed}:pct")
        self._priorities: dict[int, float] = {}
        self._floor = 0.0  # change-point demotions go ever lower
        # Sampled without replacement: two change points on the same step
        # would collapse into one demotion and quietly cost a level of the
        # depth the caller asked for.
        self._change_points = frozenset(self._rng.sample(range(horizon), depth - 1))
        self._step_count = 0
        self.diverged_at: int | None = None  # nothing to replay, nothing to depart from

    def _priority(self, owner: int) -> float:
        # Priorities live in [1, 2) and demotions strictly below 1, so a
        # demoted owner loses to every owner still holding its first draw.
        # The map grows with owners *seen* and is never pruned. Tasks
        # contribute one entry each however many steps they take, but every
        # unowned callback is an owner of its own, so the real bound is one
        # entry per distinct entry the policy was shown — the length of the
        # run, not the number of tasks in it. It is still bounded by a single
        # run and freed with the policy, which is why nothing prunes it.
        prio = self._priorities.get(owner)
        if prio is None:
            prio = self._priorities[owner] = self._rng.random() + 1.0
        return prio

    def choose(self, ready: Sequence[ReadyView]) -> int:
        step = self._step_count
        self._step_count += 1
        # Every entry is read, so every owner in the queue is priced before
        # the comparison — first sight of an owner draws here, in queue
        # order, which is what makes the draws reproducible. Two owners can
        # in principle draw equal floats; ``max`` keeps the earliest index,
        # so even then the choice is defined rather than arbitrary.
        index = max(range(len(ready)), key=lambda i: self._priority(ready[i][0]))
        if step in self._change_points:
            self._floor -= 1.0
            self._priorities[ready[index][0]] = self._floor
        return index
