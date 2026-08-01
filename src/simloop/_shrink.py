"""Reduce a failing run's schedule to the choices that actually caused it.

A recorded choice list says what the scheduler did at every step, and almost
none of it matters: a race is a handful of steps that had to go one specific
way, buried in thousands that could have gone any way at all. Shrinking puts
back as much FIFO order as it can — choice 0 at every step, the order
callbacks were scheduled in — and reports what had to stay.

The judge is the exception, never the trace hash. Editing a schedule changes
the trace by definition, so hash comparison would reject every candidate;
instead a candidate counts as the same failure when it raises the same type
with the same message prefix. Candidates that drift far enough for the
recording to stop describing the run are not an error either: the scripted
policy clamps out-of-range choices and falls back to FIFO once it runs out,
so every candidate completes, and the ones that stop failing are simply
rejected.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from simloop._loop import SimLoop
from simloop._policy import ReadyView, SchedulingPolicy
from simloop._run import Workload, finish, run_once
from simloop._trace import TraceRecorder

DEFAULT_BUDGET = 500

# How much of a failure's message has to match for it to count as the same
# failure: long enough to tell two failures apart, short enough to survive
# the values messages tend to end with, which move with the schedule.
_MESSAGE_PREFIX = 80

_Probe = Callable[[tuple[int, ...]], bool]


@dataclass(frozen=True, slots=True)
class ShrinkResult:
    """A failing schedule reduced to the choices that still reproduce it.

    ``choices`` replays under ``SimLoop._from_choices`` on the original seed
    and fails the same way. Every step outside ``kept`` runs FIFO, so ``kept``
    is the whole of what the scheduler had to get wrong, ``labels`` is what
    ran at each of those steps, and ``fifo_prefix`` counts the leading steps
    that needed no choice at all. ``oracle_runs`` is what the search spent.
    """

    original_len: int
    choices: tuple[int, ...]
    fifo_prefix: int
    kept: tuple[int, ...]
    labels: tuple[str, ...]
    oracle_runs: int


def shrink_schedule(
    fn: Workload,
    seed: int,
    choices: Sequence[int],
    failure: Exception,
    *,
    budget: int = DEFAULT_BUDGET,
) -> ShrinkResult | None:
    """Minimize ``choices`` to the scheduling decisions that keep ``failure``.

    Three passes, cheapest first: drop the recording past the point the
    failure fired, hand the longest possible prefix back to FIFO, then
    delta-debug what is left. Each pass narrows what the next one searches.

    ``budget`` caps how many candidate schedules are run. Running out is not
    an error: the best candidate found so far is returned whatever pass the
    search was in the middle of. Returns ``None`` only when the recording
    itself does not reproduce ``failure``, which leaves nothing to minimize.
    """
    if budget < 1:
        raise ValueError("shrink budget must be at least 1")
    recorded = tuple(choices)
    search = _Search(fn, seed, failure, budget)
    try:
        if not search.probe(recorded):
            return None
        length = _shortest_prefix(recorded, search.probe)
        prefix = _fifo_prefix(recorded, length, search.probe)
        _ddmin(recorded, length, range(prefix, length), search.probe)
    except _OutOfBudget:
        pass
    best = search.best
    assert best is not None, "the recording probed as a failure"
    kept = tuple(step for step, choice in enumerate(best) if choice)
    # Trailing FIFO choices are exactly what an exhausted recording falls back
    # to, so dropping them cannot change how the candidate runs.
    trimmed = best[: kept[-1] + 1] if kept else ()
    return ShrinkResult(
        original_len=len(recorded),
        choices=trimmed,
        fifo_prefix=kept[0] if kept else 0,
        kept=kept,
        labels=_labels_at(fn, seed, trimmed, kept),
        oracle_runs=search.runs,
    )


def _same_failure(original: Exception, candidate: Exception | None) -> bool:
    """Did ``candidate`` fail the way ``original`` did?

    Exact type, then the head of the message. A subclass is a different
    failure: raising ``TimeoutError`` where the recording raised
    ``OSError`` is news, not a reproduction.
    """
    if candidate is None:
        return False
    if type(candidate) is not type(original):
        return False
    return str(candidate)[:_MESSAGE_PREFIX] == str(original)[:_MESSAGE_PREFIX]


class _OutOfBudget(Exception):
    """The search may run no more candidates; unwinds to the best so far."""


class _Search:
    """Runs candidate schedules and remembers the best one that still failed.

    Best means least work left for the recording to do: the fewest steps
    holding a recorded choice, then the shortest list. Every pass reports
    through here, so whatever the search was in the middle of when the budget
    ran out, the answer is still the best candidate that actually failed.
    """

    def __init__(
        self, fn: Workload, seed: int, failure: Exception, budget: int
    ) -> None:
        self._fn = fn
        self._seed = seed
        self._failure = failure
        self._budget = budget
        self.runs = 0
        self.best: tuple[int, ...] | None = None

    def probe(self, candidate: tuple[int, ...]) -> bool:
        if self.runs >= self._budget:
            raise _OutOfBudget
        self.runs += 1
        if not _same_failure(self._failure, _replay(self._fn, self._seed, candidate)):
            return False
        if self.best is None or _rank(candidate) < _rank(self.best):
            self.best = candidate
        return True


def _rank(candidate: Sequence[int]) -> tuple[int, int]:
    return (sum(1 for choice in candidate if choice), len(candidate))


def _replay(fn: Workload, seed: int, choices: Sequence[int]) -> Exception | None:
    """Run the workload once more with ``choices`` driving the scheduler."""
    loop = SimLoop._from_choices(choices, seed)
    try:
        return run_once(loop, fn)
    finally:
        finish(loop)


# ----------------------------------------------------------------------
# Reduction passes
# ----------------------------------------------------------------------


def _candidate(
    recorded: Sequence[int], length: int, kept: Iterable[int]
) -> tuple[int, ...]:
    """A ``length``-step schedule: recorded choices at ``kept``, FIFO elsewhere."""
    choices = [0] * length
    for step in kept:
        choices[step] = recorded[step]
    return tuple(choices)


def _shortest_prefix(recorded: Sequence[int], probe: _Probe) -> int:
    """Fewest leading choices that still reproduce the failure.

    Everything the recording says about the steps after the failure fired is
    irrelevant, and a truncated recording runs FIFO from where it ends rather
    than stopping, so truncation is safe as well as cheap. Monotonicity — a
    longer prefix reproduces whatever a shorter one did — is a heuristic, but
    it is the one that makes this a binary search instead of a scan.
    """
    lo, hi = 0, len(recorded)
    while lo < hi:
        mid = (lo + hi) // 2
        if probe(tuple(recorded[:mid])):
            hi = mid
        else:
            lo = mid + 1
    return hi


def _fifo_prefix(recorded: Sequence[int], length: int, probe: _Probe) -> int:
    """Most leading steps that can run FIFO with the failure still firing.

    Searched from a known-failing zero — the truncation the previous pass
    settled on — upwards, so the pass can only improve on what it was given.
    """
    lo, hi = 0, length
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if probe(_candidate(recorded, length, range(mid, length))):
            lo = mid
        else:
            hi = mid - 1
    return lo


def _ddmin(
    recorded: Sequence[int],
    length: int,
    positions: Iterable[int],
    probe: _Probe,
) -> tuple[int, ...]:
    """Delta-debug which of ``positions`` must keep their recorded choice.

    Zeller's ddmin over the surviving window: try each chunk on its own,
    then each chunk's complement, doubling the granularity when neither
    helps. The result is 1-minimal — reverting any single remaining step to
    FIFO stops the failure — which is as far as this can go without
    searching every subset.
    """
    kept = tuple(positions)
    granularity = 2
    while len(kept) > 1:
        chunks = _split(kept, granularity)
        for chunk in chunks:
            if probe(_candidate(recorded, length, chunk)):
                kept, granularity = chunk, 2
                break
        else:
            # At granularity 2 the complements are the chunks themselves,
            # which the loop above has just ruled out.
            rest = (
                _first_failing_complement(recorded, length, kept, chunks, probe)
                if granularity > 2
                else None
            )
            if rest is not None:
                kept, granularity = rest, max(granularity - 1, 2)
            elif granularity >= len(kept):
                break
            else:
                granularity = min(granularity * 2, len(kept))
    return kept


def _first_failing_complement(
    recorded: Sequence[int],
    length: int,
    kept: tuple[int, ...],
    chunks: list[tuple[int, ...]],
    probe: _Probe,
) -> tuple[int, ...] | None:
    for chunk in chunks:
        dropped = set(chunk)
        rest = tuple(step for step in kept if step not in dropped)
        if rest and probe(_candidate(recorded, length, rest)):
            return rest
    return None


def _split(items: tuple[int, ...], count: int) -> list[tuple[int, ...]]:
    """Cut ``items`` into ``count`` near-equal chunks, dropping empty ones."""
    chunks = []
    for index in range(count):
        chunk = items[index * len(items) // count : (index + 1) * len(items) // count]
        if chunk:
            chunks.append(chunk)
    return chunks


# ----------------------------------------------------------------------
# Naming the kept steps
# ----------------------------------------------------------------------


class _StepMarks:
    """Notes where in the trace each step's own event lands.

    The loop records a step's ``run`` or ``cancel`` event immediately after
    asking the policy to choose, so the number of events recorded at the
    moment of the call is the index of the event that choice selected.
    Counting event kinds afterwards would not do: advancing the clock records
    events of its own, including cancellations, and nothing in the trace
    marks where one step ends and the next begins.
    """

    def __init__(self, inner: SchedulingPolicy, recorder: TraceRecorder) -> None:
        self._inner = inner
        self._recorder = recorder
        self.diverged_at: int | None = None
        self.at: list[int] = []

    def choose(self, ready: Sequence[ReadyView]) -> int:
        self.at.append(len(self._recorder))
        choice = self._inner.choose(ready)
        self.diverged_at = self._inner.diverged_at
        return choice


def _labels_at(
    fn: Workload, seed: int, choices: Sequence[int], kept: Sequence[int]
) -> tuple[str, ...]:
    """Re-run the minimized schedule and name what ran at each kept step.

    A run of its own rather than a trace held through the search: the search
    can spend hundreds of runs, and holding the trace of every candidate that
    briefly looked best would cost far more than replaying the winner once.
    This run is outside the budget, which counts the search's decisions.
    """
    if not kept:
        return ()
    loop = SimLoop._from_choices(choices, seed)
    marks = _StepMarks(loop._policy, loop._recorder)
    loop._policy = marks
    try:
        run_once(loop, fn)
        events = loop.trace
    finally:
        finish(loop)
    labels = []
    for step in kept:
        at = marks.at[step] if step < len(marks.at) else len(events)
        labels.append(events[at].label if at < len(events) else "?")
    return tuple(labels)
