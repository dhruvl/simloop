"""Run a simulation test under many seeds and report the first failure.

This module is plain library code: it never imports pytest. The pytest
integration in ``_pytest_plugin`` feeds session options in through the
module-level ``overrides`` object, which keeps ``import simloop`` free of
any test-framework dependency.
"""

from __future__ import annotations

import functools
import inspect
import math
import os
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, replace
from typing import Any, overload

from simloop._loop import SimLoop
from simloop._parallel import (
    ModuleWorkload,
    check_reproduced,
    find_lowest_failure,
)
from simloop._policy import PCTPolicy, SchedulingPolicy
from simloop._run import Workload, finish, run_once
from simloop._shrink import DEFAULT_BUDGET, ShrinkResult, shrink_schedule
from simloop._trace import EventKind, TraceEvent

# How the scheduler may be driven while exploring: seeded uniform draws, or
# PCT's priorities.
POLICIES = ("random", "pct")
# Ordering constraints PCT aims to hit; the paper's own default.
DEFAULT_PCT_DEPTH = 3
# The seed whose run measures the workload for PCT. Fixed rather than "the
# first seed asked for", so that the horizon depends on the workload alone:
# replaying one found seed on its own has to size the horizon exactly as the
# search that found it did, or the replay runs a different schedule.
_CALIBRATION_SEED = 0
# Room left above the measured step count, so that a run a little longer than
# the measured one still has change points falling inside it.
_HORIZON_SLACK = 1.5
# Below this, a horizon is too small to place change points with any spread,
# and the measurement is too short to be worth trusting anyway.
_MIN_HORIZON = 100

# Events of context shown either side of the point two runs part ways.
_CONTEXT = 5
# Below this much agreement the split point sits in the runs' opening
# bookkeeping, where a window of context says nothing about the workload.
_MIN_PREFIX = 5
# Landmarks to align on instead, most informative first.
_ANCHOR_KINDS: tuple[EventKind, ...] = ("net", "advance")
# Kept steps listed before the shrink block starts summarizing.
_KEPT_SHOWN = 12


@dataclass(frozen=True, slots=True)
class PendingTask:
    """One task still pending when a seed failed."""

    host: str
    name: str
    awaiting: str
    where: str


@dataclass(frozen=True, slots=True)
class Divergence:
    """Where a failing run's schedule parted from a passing run's.

    ``prefix_len`` counts the leading events the two runs agree on, compared
    by kind and label only: ``when`` and ``seq`` legitimately differ once the
    interleaving does, so including them would report every run as diverging
    at its first timer. ``passing_next`` and ``failing_next`` are what each
    run did at the split point, or ``None`` for a run that ended there.

    The context windows normally surround the split point. When the runs
    split too early for that to carry any context, they surround a shared
    landmark named by ``anchor`` instead; both windows are empty when no
    landmark was worth showing either.
    """

    prefix_len: int
    anchor: str | None
    passing_next: TraceEvent | None
    failing_next: TraceEvent | None
    passing_context: tuple[TraceEvent, ...]
    failing_context: tuple[TraceEvent, ...]


@dataclass(frozen=True, slots=True)
class PolicyRun:
    """Which scheduler drove one seed, when it was not the loop's own.

    Only ``"pct"`` reaches here today: the default policy is the loop's own
    seeded draw, and a run under it carries no record at all, so nothing about
    a default exploration changes.

    ``horizon`` is the step count PCT sized its change points against, and is
    ``None`` for the seed the measurement is taken from — that seed runs the
    seeded schedule, because a measurement of a PCT run would be a measurement
    of a schedule chosen by the number it is supposed to produce.
    """

    name: str
    depth: int
    horizon: int | None = None

    def build(self, seed: int) -> SchedulingPolicy | None:
        """The policy for ``seed``, or ``None`` to leave the loop's own in place."""
        if self.horizon is None:
            return None
        return PCTPolicy(seed, self.depth, self.horizon)

    def replay_options(self) -> str:
        """The --simloop-* options a replay of this seed needs to match it."""
        options = f" --simloop-policy={self.name}"
        if self.depth != DEFAULT_PCT_DEPTH:
            options += f" --simloop-pct-depth={self.depth}"
        return options


@dataclass(frozen=True, slots=True)
class SeedReport:
    """Everything known about the first failing seed."""

    seed: int
    seeds_passed: int
    exception: Exception
    trace_events: tuple[TraceEvent, ...]
    trace_hash: str
    pending: tuple[PendingTask, ...]
    divergence: Divergence | None = None
    shrunk: ShrinkResult | None = None
    policy: PolicyRun | None = None

    def render(self, test_id: str | None = None) -> str:
        lines = [
            f"simloop: failed at seed {self.seed} "
            f"({self.seeds_passed} seeds passed first)"
        ]
        if test_id is not None:
            options = self.policy.replay_options() if self.policy else ""
            lines.append(
                f"replay: pytest '{test_id}' --simloop-replay={self.seed}{options}"
            )
        if self.policy is not None:
            lines.append(_render_policy(self.policy))
        if self.trace_events:
            lines.append("")
            lines.append(f"last {len(self.trace_events)} trace events:")
            lines.extend(_format_event(event) for event in self.trace_events)
        if self.divergence is not None:
            lines.append("")
            lines.extend(_render_divergence(self.divergence))
        if self.shrunk is not None:
            lines.append("")
            lines.extend(_render_shrink(self.shrunk))
        if self.pending:
            lines.append("pending tasks by host:")
            for task in self.pending:
                lines.append(
                    f"  {task.host}  Task {task.name!r}  "
                    f"awaiting {task.awaiting}  at {task.where}"
                )
        return "\n".join(lines)


def _format_event(event: TraceEvent) -> str:
    # The host goes in a column of its own ahead of the label, the way the
    # pending-task block names machines. Events that belong to the simulation
    # rather than to a machine — a clock advance, a packet on the wire — carry
    # no host and keep the two-space gap they always had.
    host = f"{event.host}  " if event.host else ""
    return (
        f"  [t={event.when:.4f}] {event.kind:<8} "
        f"seq={event.seq}  {host}{event.label}"
    )


def _render_policy(policy: PolicyRun) -> str:
    if policy.horizon is None:
        return (
            f"policy: {policy.name} (depth {policy.depth}); this seed ran the "
            "default schedule, which is what sized the horizon"
        )
    return f"policy: {policy.name} (depth {policy.depth}, horizon {policy.horizon:,})"


def _describe(event: TraceEvent | None, subject: str) -> str:
    if event is None:
        return f"{subject} ended"
    # Clock advances carry no label; their kind is the whole story.
    return f"{subject} ran {event.label or event.kind}"


def _render_divergence(divergence: Divergence) -> list[str]:
    if not divergence.passing_context and not divergence.failing_context:
        return ["schedules diverge immediately"]
    noun = "event" if divergence.prefix_len == 1 else "events"
    lines = [
        f"runs agree for {divergence.prefix_len:,} {noun}; "
        f"{_describe(divergence.passing_next, 'passing then')}, "
        f"{_describe(divergence.failing_next, 'failing')}"
    ]
    if divergence.anchor is not None:
        lines.append(f"context anchored at the {divergence.anchor}")
    lines.append("passing run:")
    lines.extend(_format_event(event) for event in divergence.passing_context)
    lines.append("failing run:")
    lines.extend(_format_event(event) for event in divergence.failing_context)
    return lines


def _render_shrink(result: ShrinkResult) -> list[str]:
    noun = "step" if result.original_len == 1 else "steps"
    runs = "run" if result.oracle_runs == 1 else "runs"
    lines = [
        f"schedule shrink (experimental): {result.original_len:,} {noun} "
        f"recorded, {result.oracle_runs:,} {runs} to minimize"
    ]
    if not result.kept:
        # Nothing about the interleaving mattered: the failure reproduces
        # with the scheduler taking the ready queue in order.
        lines.append("minimized: FIFO throughout")
        return lines
    first, last = result.kept[0], result.kept[-1]
    span = f"step {first:,}" if first == last else f"steps {first:,}-{last:,}"
    lines.append(f"minimized: FIFO except {span}")
    for step, label in zip(result.kept[:_KEPT_SHOWN], result.labels):
        lines.append(f"  step {step:,}  {label}")
    hidden = len(result.kept) - _KEPT_SHOWN
    if hidden > 0:
        lines.append(f"  ... and {hidden:,} more")
    return lines


def _diff_traces(
    passing: tuple[TraceEvent, ...],
    failing: tuple[TraceEvent, ...],
    *,
    context: int = _CONTEXT,
) -> Divergence | None:
    """Find the first scheduling decision two runs disagreed on.

    Returns ``None`` when there is nothing to explain: one of the runs left
    no trace, or both made exactly the same decisions, which is the normal
    case for a failure the interleaving did not cause.
    """
    if not passing or not failing:
        return None
    split = 0
    for left, right in zip(passing, failing):
        if (left.kind, left.label) != (right.kind, right.label):
            break
        split += 1
    if split == len(passing) == len(failing):
        return None
    passing_next = passing[split] if split < len(passing) else None
    failing_next = failing[split] if split < len(failing) else None
    if split >= _MIN_PREFIX:
        at_passing, at_failing = split, split
        anchor = None
    else:
        anchored = _anchor(passing, failing)
        if anchored is None:
            return Divergence(split, None, passing_next, failing_next, (), ())
        anchor, at_passing, at_failing = anchored
    return Divergence(
        prefix_len=split,
        anchor=anchor,
        passing_next=passing_next,
        failing_next=failing_next,
        passing_context=_window(passing, at_passing, context),
        failing_context=_window(failing, at_failing, context),
    )


def _anchor(
    passing: tuple[TraceEvent, ...], failing: tuple[TraceEvent, ...]
) -> tuple[str, int, int] | None:
    """Pick a landmark both runs reached, to align their windows on."""
    for kind in _ANCHOR_KINDS:
        at_passing = _first_of_kind(passing, kind)
        at_failing = _first_of_kind(failing, kind)
        if at_passing is None or at_failing is None:
            continue
        if at_passing == 0 and at_failing == 0:
            # Aligning here would reprint the head of each trace, which is
            # what a window at the split point already showed.
            continue
        return f"first {kind} event", at_passing, at_failing
    return None


def _first_of_kind(events: tuple[TraceEvent, ...], kind: EventKind) -> int | None:
    for index, event in enumerate(events):
        if event.kind == kind:
            return index
    return None


def _window(
    events: tuple[TraceEvent, ...], at: int, context: int
) -> tuple[TraceEvent, ...]:
    return events[max(0, at - context) : at + context]


def explore(
    fn: Workload,
    seeds: Iterable[int],
    *,
    trace_tail: int = 20,
    shrink: bool = False,
    shrink_budget: int = DEFAULT_BUDGET,
    jobs: int = 1,
    policy: str = "random",
    pct_depth: int = DEFAULT_PCT_DEPTH,
) -> SeedReport | None:
    """Run ``fn`` once per seed on a fresh SimLoop; stop at the first failure.

    Returns a :class:`SeedReport` for the first seed whose run raised an
    ``Exception``, or ``None`` when every seed passed. ``BaseException``s
    that are not test failures (``KeyboardInterrupt``, ``SystemExit``)
    propagate immediately.

    The most recent passing seed's full trace is held for comparison against
    the failing one. Only the last is kept, so the extra memory is one trace
    however many seeds are explored, and both traces are snapshotted before
    teardown so that teardown scheduling cannot show up as a divergence.

    ``shrink`` minimizes the failing seed's schedule, spending at most
    ``shrink_budget`` further runs of ``fn`` on it. It is off by default
    because it is experimental and, unlike everything else in the report,
    not free.

    ``jobs`` above 1 spreads the seeds over that many worker processes. The
    report does not change — it describes the seed sequential exploration
    would have stopped at, rebuilt by re-running that seed here — but the
    workload has to survive pickling to reach a worker at all, so it must be
    a module-level function or a partial of one.

    ``policy="pct"`` schedules by priority instead of drawing uniformly,
    which buys a per-run probability bound on finding a bug that needs
    ``pct_depth`` ordering constraints. It costs one extra run of the
    workload: seed 0 runs the seeded schedule, and its step count is what the
    priority policy's horizon is sized from. That seed is fixed rather than
    "whichever seed came first", so the horizon describes the workload alone
    — which is what lets one found seed, replayed on its own, run the very
    schedule that found it.
    """
    if jobs < 1:
        raise ValueError("jobs must be at least 1")
    if policy not in POLICIES:
        raise ValueError(
            f"unknown scheduling policy {policy!r}: expected one of "
            f"{', '.join(repr(name) for name in POLICIES)}"
        )
    if policy == "pct":
        if pct_depth < 1:
            raise ValueError(f"pct_depth must be at least 1, got {pct_depth}")
        if jobs > 1:
            # Refused rather than quietly ignored: seed 0 sizes the horizon in
            # this process, and the parallel search hands workers a seed and
            # nothing else, so workers would explore uniformly while the report
            # claimed PCT.
            raise ValueError(
                "policy='pct' explores sequentially, because the horizon it "
                "sizes here does not reach worker processes: run it with jobs=1"
            )
    if jobs > 1:
        ordered = list(seeds)
        if len(ordered) > 1:
            return _explore_parallel(
                fn,
                ordered,
                jobs=jobs,
                trace_tail=trace_tail,
                shrink=shrink,
                shrink_budget=shrink_budget,
            )
        # One seed is one run: worth no process pool at all.
        seeds = ordered
    passed = 0
    horizon: int | None = None
    last_pass: tuple[TraceEvent, ...] = ()
    for seed in seeds:
        if policy == "pct" and horizon is None and seed != _CALIBRATION_SEED:
            # Measured on first need rather than up front, so that a caller
            # who asked for no seeds — or only for the measuring seed, which
            # measures itself — runs the workload exactly as often as asked.
            horizon = _pct_horizon(_measure_steps(fn), pct_depth)
        report, events = _run_seed(
            fn,
            seed,
            passed,
            last_pass,
            trace_tail=trace_tail,
            shrink=shrink,
            shrink_budget=shrink_budget,
            policy=_policy_for(policy, pct_depth, horizon, seed),
        )
        if report is not None:
            return report
        last_pass = events
        passed += 1
    return None


def _policy_for(
    policy: str, depth: int, horizon: int | None, seed: int
) -> PolicyRun | None:
    """How ``seed`` should be scheduled, or ``None`` for the loop's own draw."""
    if policy == "random":
        return None
    # The measuring seed keeps the seeded schedule, and says so by carrying no
    # horizon: it is the run the horizon was read off, and it is explored like
    # every other seed rather than being spent on measurement alone.
    return PolicyRun(
        policy, depth, None if seed == _CALIBRATION_SEED else horizon
    )


def _measure_steps(fn: Workload) -> int:
    """How many scheduling steps the workload takes under the seeded schedule.

    One run of seed 0, whose outcome is deliberately dropped: this is a
    measurement, not an exploration, and seed 0 is explored on its own account
    when it is one of the seeds asked for. The choice log is one entry per
    step, which is the unit a horizon is counted in; the trace is several
    events per step and would size the horizon several times too large.
    """
    loop = SimLoop(_CALIBRATION_SEED)
    try:
        run_once(loop, fn)
        return len(loop._choices)
    finally:
        finish(loop)


def _pct_horizon(steps: int, depth: int) -> int:
    """Size a PCT horizon from a measured step count.

    Slack above the measurement, then a floor, because a horizon shorter than
    the run leaves its tail at fixed priorities and a horizon of a handful of
    steps cannot spread change points at all. The last clamp is the one that
    is not about tuning: PCT needs ``depth - 1`` distinct change points and
    refuses to be built without room for them, so a deep search of a short
    workload widens the horizon rather than failing.
    """
    horizon = max(math.ceil(steps * _HORIZON_SLACK), _MIN_HORIZON)
    return max(horizon, depth - 1)


def _run_seed(
    fn: Workload,
    seed: int,
    seeds_passed: int,
    last_pass: tuple[TraceEvent, ...],
    *,
    trace_tail: int,
    shrink: bool,
    shrink_budget: int,
    policy: PolicyRun | None = None,
) -> tuple[SeedReport | None, tuple[TraceEvent, ...]]:
    """Run one seed; report it if it failed, and hand back its trace either way.

    The trace is the caller's business: a passing seed's is what the next
    failure gets diffed against.
    """
    loop = SimLoop(seed)
    if policy is not None:
        scheduler = policy.build(seed)
        if scheduler is not None:
            # Built by the seed, then scheduled by something else: the clock,
            # the network faults and the user-facing entropy stay exactly what
            # the seed put there, and only the order of the ready queue moves.
            # This is the seam SimLoop._from_choices uses for replay.
            loop._policy = scheduler
    try:
        failure = run_once(loop, fn)
        events = loop.trace
        if failure is None:
            return None, events
        report = SeedReport(
            seed=seed,
            seeds_passed=seeds_passed,
            exception=failure,
            trace_events=events[-trace_tail:] if trace_tail else (),
            trace_hash=loop.trace_hash(),
            pending=_pending_tasks(loop),
            divergence=_diff_traces(last_pass, events),
            policy=policy,
        )
        choices = loop._choices
    finally:
        finish(loop)
    if not shrink:
        return report, events
    # Shrinking runs the workload many more times, so it happens with the
    # failing seed's own loop already torn down.
    return (
        replace(
            report,
            shrunk=shrink_schedule(fn, seed, choices, failure, budget=shrink_budget),
        ),
        events,
    )


def _explore_parallel(
    fn: Workload,
    seeds: list[int],
    *,
    jobs: int,
    trace_tail: int,
    shrink: bool,
    shrink_budget: int,
) -> SeedReport | None:
    """Search ``seeds`` in worker processes, then build the report here.

    Workers answer only with which seed failed and how, so the report is
    made the way it always was: by running the failing seed, and the seed
    below it for the schedule diff, on this process's own loops. Two runs
    on top of the search — and both of them check that the seed does the
    same thing here as it did in the worker, which is the one thing a
    parallel run can get away with quietly breaking.
    """
    found = find_lowest_failure(fn, seeds, jobs=jobs)
    if found is None:
        return None
    last_pass: tuple[TraceEvent, ...] = ()
    if found.at > 0:
        below = seeds[found.at - 1]
        passing, last_pass = _run_seed(
            fn,
            below,
            found.at - 1,
            (),
            trace_tail=trace_tail,
            shrink=False,
            shrink_budget=shrink_budget,
        )
        check_reproduced(
            below, None, passing.exception if passing is not None else None
        )
    seed = seeds[found.at]
    report, _ = _run_seed(
        fn,
        seed,
        found.at,
        last_pass,
        trace_tail=trace_tail,
        shrink=shrink,
        shrink_budget=shrink_budget,
    )
    check_reproduced(
        seed, found, report.exception if report is not None else None
    )
    assert report is not None, "the parent reproduced the worker's failure"
    return report


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


@dataclass
class _Overrides:
    """Session state the pytest plugin writes; consulted by sim_test wrappers.

    ``seeds``, ``replay``, ``shrink``, ``shrink_budget``, ``jobs``,
    ``policy`` and ``pct_depth`` mirror the --simloop-* options; ``node_id``
    is the test currently running, so reports can print an exact replay
    command. The counters feed the plugin's terminal summary.

    ``policy`` and ``pct_depth`` are ``None`` when the option was not given,
    the way ``seeds`` is: an option nobody passed must leave a decorator's own
    argument alone, while ``--simloop-policy=random`` is a decision and
    overrides it.
    """

    seeds: int | None = None
    replay: int | None = None
    shrink: bool = False
    shrink_budget: int = DEFAULT_BUDGET
    jobs: int = 1
    policy: str | None = None
    pct_depth: int | None = None
    node_id: str | None = None
    sim_tests: int = 0
    seeds_explored: int = 0


overrides = _Overrides()

_TestFn = Callable[..., Coroutine[Any, Any, object]]


def _worker_workload(
    test_fn: _TestFn, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Workload:
    """Address a decorated test so that worker processes can find it.

    The wrapper a worker would have to receive is a closure over
    ``test_fn``, and closures do not pickle; a file and a name do. What
    that rules out is a test whose arguments the workers would also have to
    reconstruct, which means fixtures: those stay sequential, loudly rather
    than by quietly running one process after all.
    """
    where = inspect.getsourcefile(test_fn)
    name = test_fn.__qualname__
    if args or kwargs:
        raise TypeError(
            f"{name} takes arguments, and pytest fixtures cannot be rebuilt "
            "in a worker process: run it without --simloop-jobs"
        )
    if where is None or "." in name:
        raise TypeError(
            f"{name} is not importable by name from a file, so worker "
            "processes cannot reach it: run it without --simloop-jobs"
        )
    return ModuleWorkload(os.path.abspath(where), test_fn.__name__)


@overload
def sim_test(fn: _TestFn, /) -> Callable[..., None]: ...


@overload
def sim_test(
    *,
    seeds: int = ...,
    trace_tail: int = ...,
    policy: str = ...,
    pct_depth: int = ...,
) -> Callable[[_TestFn], Callable[..., None]]: ...


def sim_test(
    fn: _TestFn | None = None,
    /,
    *,
    seeds: int = 10,
    trace_tail: int = 20,
    policy: str = "random",
    pct_depth: int = DEFAULT_PCT_DEPTH,
) -> Callable[..., None] | Callable[[_TestFn], Callable[..., None]]:
    """Turn an ``async def`` test into a seed-exploring synchronous test.

    The wrapper runs the coroutine under ``seeds`` seeds (0..N-1) via
    :func:`explore` and re-raises the first failure with the rendered
    report attached as an exception note. Under pytest, the --simloop-seeds
    and --simloop-replay options override the decorator's arguments,
    --simloop-shrink adds a minimized schedule to the report,
    --simloop-jobs spreads the seeds over worker processes, and
    --simloop-policy / --simloop-pct-depth override ``policy`` and
    ``pct_depth``.
    """
    if seeds < 1:
        raise ValueError("seeds must be at least 1")
    if policy not in POLICIES:
        raise ValueError(
            f"unknown scheduling policy {policy!r}: expected one of "
            f"{', '.join(repr(name) for name in POLICIES)}"
        )

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
            jobs = overrides.jobs
            workload: Workload = (
                _worker_workload(test_fn, args, kwargs)
                if jobs > 1 and len(seed_set) > 1
                else functools.partial(test_fn, *args, **kwargs)
            )
            report = explore(
                workload,
                seed_set,
                trace_tail=trace_tail,
                shrink=overrides.shrink,
                shrink_budget=overrides.shrink_budget,
                jobs=jobs,
                policy=policy if overrides.policy is None else overrides.policy,
                pct_depth=(
                    pct_depth if overrides.pct_depth is None else overrides.pct_depth
                ),
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
