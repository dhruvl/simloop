"""Explorer core: first-failure seed search over fresh SimLoops."""

import asyncio
import subprocess
import sys

import pytest

import simloop
from simloop import SeedReport, TraceEvent, sim_test
from simloop._explore import (
    Divergence,
    PolicyRun,
    _diff_traces,
    _format_event,
    _pct_horizon,
    explore,
)
from simloop._policy import PCTPolicy, SchedulingPolicy
from simloop._run import finish, run_once
from simloop._trace import EventKind


async def _fails_at(bad_seed: int) -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    await asyncio.sleep(1.0)
    if loop.seed == bad_seed:
        raise RuntimeError("boom")


def test_explore_reports_first_failing_seed() -> None:
    report = explore(lambda: _fails_at(3), range(10))
    assert report is not None
    assert report.seed == 3
    assert report.seeds_passed == 3
    assert isinstance(report.exception, RuntimeError)
    assert str(report.exception) == "boom"


def test_explore_returns_none_when_all_seeds_pass() -> None:
    assert explore(lambda: _fails_at(99), range(10)) is None


def test_explore_is_deterministic() -> None:
    first = explore(lambda: _fails_at(7), range(10))
    second = explore(lambda: _fails_at(7), range(10))
    assert first is not None and second is not None
    assert first.seed == second.seed
    assert first.trace_hash == second.trace_hash
    assert first.trace_events == second.trace_events


def test_trace_tail_is_bounded() -> None:
    report = explore(lambda: _fails_at(0), range(1), trace_tail=5)
    assert report is not None
    assert len(report.trace_events) == 5
    assert report.trace_events[-1].kind in ("run", "cancel", "advance", "schedule", "net")


async def _interrupt() -> None:
    raise KeyboardInterrupt


def test_base_exceptions_propagate() -> None:
    with pytest.raises(KeyboardInterrupt):
        explore(lambda: _interrupt(), range(3))


async def _leaves_a_pending_task() -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    loop.net.host("node1").create_task(_waits_forever(), name="stuck")
    await asyncio.sleep(1.0)
    raise RuntimeError("boom")


async def _waits_forever() -> None:
    await asyncio.Event().wait()


def test_failed_run_leaves_no_stderr_noise(
    capfd: pytest.CaptureFixture[str],
) -> None:
    # A failed seed abandons its pending tasks; the explorer must tear them
    # down so their garbage collection cannot write "Task was destroyed"
    # to stderr after the run.
    report = explore(lambda: _leaves_a_pending_task(), range(1))
    assert report is not None
    import gc

    gc.collect()
    _, err = capfd.readouterr()
    assert err == ""


def test_render_includes_seed_trace_and_pending() -> None:
    report = explore(lambda: _leaves_a_pending_task(), range(1), trace_tail=5)
    assert report is not None
    text = report.render("tests/test_demo.py::test_x")
    lines = text.splitlines()
    assert lines[0] == "simloop: failed at seed 0 (0 seeds passed first)"
    assert lines[1] == (
        "replay: pytest 'tests/test_demo.py::test_x' --simloop-replay=0"
    )
    assert "last 5 trace events:" in text
    assert "pending tasks by host:" in text
    assert "node1" in text and "'stuck'" in text
    assert "awaiting _waits_forever" in text


def test_render_without_test_id_omits_replay_line() -> None:
    report = explore(lambda: _fails_at(0), range(1))
    assert report is not None
    text = report.render()
    assert "replay:" not in text
    assert text.startswith("simloop: failed at seed 0")


def _ev(kind: EventKind, label: str, seq: int = 0) -> TraceEvent:
    return TraceEvent(kind, float(seq), seq, label)


def _steps(*labels: str) -> tuple[TraceEvent, ...]:
    return tuple(_ev("run", label, index) for index, label in enumerate(labels))


def test_diff_returns_none_for_identical_schedules() -> None:
    events = _steps("a", "b", "c")
    assert _diff_traces(events, events) is None


def test_diff_ignores_time_and_sequence_numbers() -> None:
    passing = (_ev("run", "a", 0), _ev("run", "b", 1))
    failing = (_ev("run", "a", 40), _ev("run", "b", 41))
    assert _diff_traces(passing, failing) is None


def test_diff_returns_none_for_empty_traces() -> None:
    assert _diff_traces((), _steps("a")) is None
    assert _diff_traces(_steps("a"), ()) is None


def test_diff_reports_a_split_in_the_middle() -> None:
    shared = [f"step{index}" for index in range(8)]
    passing = _steps(*shared, "worker.renew", "worker.ack")
    failing = _steps(*shared, "Broker.expire", "Broker.sweep")
    diff = _diff_traces(passing, failing)
    assert diff is not None
    assert diff.prefix_len == 8
    assert diff.anchor is None
    assert diff.passing_next is not None
    assert diff.passing_next.label == "worker.renew"
    assert diff.failing_next is not None
    assert diff.failing_next.label == "Broker.expire"
    assert diff.passing_context == passing[3:10]
    assert diff.failing_context == failing[3:10]


def test_diff_marks_the_run_that_ended_first() -> None:
    shared = [f"step{index}" for index in range(8)]
    passing = _steps(*shared)
    failing = _steps(*shared, "Broker.expire")
    diff = _diff_traces(passing, failing)
    assert diff is not None
    assert diff.prefix_len == 8
    assert diff.passing_next is None
    assert diff.failing_next is not None


def test_diff_anchors_on_the_first_net_event_when_runs_split_early() -> None:
    passing = (
        _ev("run", "a", 0),
        _ev("run", "b", 1),
        _ev("run", "c", 2),
        _ev("net", "send n1>n2", 3),
        _ev("run", "d", 4),
    )
    failing = (
        _ev("run", "a", 0),
        _ev("run", "z", 1),
        _ev("net", "send n1>n3", 2),
        _ev("run", "d", 3),
    )
    diff = _diff_traces(passing, failing)
    assert diff is not None
    assert diff.prefix_len == 1
    assert diff.anchor == "first net event"
    assert diff.passing_context == passing
    assert diff.failing_context == failing


def test_diff_falls_back_to_the_first_advance_event() -> None:
    passing = (
        _ev("run", "a", 0),
        _ev("run", "b", 1),
        _ev("advance", "", 2),
        _ev("run", "c", 3),
    )
    failing = (
        _ev("run", "a", 0),
        _ev("run", "z", 1),
        _ev("run", "y", 2),
        _ev("advance", "", 3),
    )
    diff = _diff_traces(passing, failing)
    assert diff is not None
    assert diff.anchor == "first advance event"
    assert diff.passing_context == passing
    assert diff.failing_context == failing


def test_diff_reports_immediate_divergence_when_no_anchor_helps() -> None:
    diff = _diff_traces(_steps("a", "b"), _steps("z", "y"))
    assert diff is not None
    assert diff.prefix_len == 0
    assert diff.anchor is None
    assert diff.passing_context == ()
    assert diff.failing_context == ()


def test_diff_rejects_an_anchor_that_adds_no_context() -> None:
    passing = (_ev("net", "send n1>n2", 0), _ev("run", "b", 1))
    failing = (_ev("net", "send n1>n3", 0), _ev("run", "b", 1))
    diff = _diff_traces(passing, failing)
    assert diff is not None
    assert diff.anchor is None
    assert diff.passing_context == ()


def _noop_a() -> None:
    pass


def _noop_b() -> None:
    pass


async def _interleaves(bad_seed: int) -> None:
    """A workload whose schedule really does depend on the seed.

    The warm-up runs with a single runnable callback, so every seed shares
    the same opening events; the rounds that follow leave three callbacks
    ready at once, which is where the seeded draws pull the runs apart.
    """
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    for _ in range(3):
        await asyncio.sleep(0)
    for _ in range(6):
        loop.call_soon(_noop_a)
        loop.call_soon(_noop_b)
        await asyncio.sleep(0)
    if loop.seed == bad_seed:
        raise RuntimeError("boom")


def _trace_of(bad_seed: int, seed: int) -> tuple[TraceEvent, ...]:
    loop = simloop.SimLoop(seed)
    try:
        try:
            loop.run_until_complete(_interleaves(bad_seed))
        except RuntimeError:
            pass
        return loop.trace
    finally:
        loop.close()


def test_explore_diffs_against_the_last_passing_seed() -> None:
    report = explore(lambda: _interleaves(3), range(10))
    assert report is not None
    assert report.seed == 3
    diff = report.divergence
    assert diff is not None
    assert diff.prefix_len >= 5
    assert diff.anchor is None
    assert diff == _diff_traces(_trace_of(3, 2), _trace_of(3, 3))
    assert _trace_of(3, 0) != _trace_of(3, 2)
    assert diff != _diff_traces(_trace_of(3, 0), _trace_of(3, 3))


def test_no_divergence_without_a_passing_seed() -> None:
    report = explore(lambda: _interleaves(0), range(4))
    assert report is not None
    assert report.seed == 0
    assert report.divergence is None
    assert "runs agree" not in report.render()


def test_no_divergence_when_the_schedules_match() -> None:
    report = explore(lambda: _fails_at(3), range(10))
    assert report is not None
    assert report.divergence is None


def test_render_shows_the_divergence_block() -> None:
    shared = [f"step{index}" for index in range(8)]
    diff = _diff_traces(
        _steps(*shared, "worker.renew"), _steps(*shared, "Broker.expire")
    )
    assert diff is not None
    text = _report_with(diff).render()
    lines = text.splitlines()
    assert (
        "runs agree for 8 events; passing then ran worker.renew, "
        "failing ran Broker.expire"
    ) in lines
    assert "passing run:" in lines
    assert "failing run:" in lines
    assert "  [t=8.0000] run      seq=8  worker.renew" in lines
    assert "  [t=8.0000] run      seq=8  Broker.expire" in lines


def test_render_groups_thousands_in_the_prefix_length() -> None:
    shared = [f"step{index}" for index in range(1200)]
    diff = _diff_traces(_steps(*shared, "left"), _steps(*shared, "right"))
    assert diff is not None
    assert "runs agree for 1,200 events;" in _report_with(diff).render()


def test_render_names_the_anchor_used() -> None:
    passing = (
        _ev("run", "a", 0),
        _ev("run", "b", 1),
        _ev("net", "send n1>n2", 2),
    )
    failing = (
        _ev("run", "a", 0),
        _ev("run", "z", 1),
        _ev("run", "y", 2),
        _ev("net", "send n1>n3", 3),
    )
    diff = _diff_traces(passing, failing)
    assert diff is not None
    text = _report_with(diff).render()
    assert "context anchored at the first net event" in text.splitlines()
    assert "runs agree for 1 event;" in text


def test_render_reports_immediate_divergence_without_context() -> None:
    diff = _diff_traces(_steps("a", "b"), _steps("z", "y"))
    assert diff is not None
    text = _report_with(diff).render()
    assert "schedules diverge immediately" in text.splitlines()
    assert "runs agree" not in text
    assert "passing run:" not in text


def test_render_describes_a_run_that_ended() -> None:
    shared = [f"step{index}" for index in range(8)]
    diff = _diff_traces(_steps(*shared), _steps(*shared, "Broker.expire"))
    assert diff is not None
    assert (
        "runs agree for 8 events; passing then ended, "
        "failing ran Broker.expire"
    ) in _report_with(diff).render()


def _report_with(divergence: Divergence) -> SeedReport:
    return SeedReport(
        seed=4,
        seeds_passed=4,
        exception=RuntimeError("boom"),
        trace_events=(),
        trace_hash="0" * 64,
        pending=(),
        divergence=divergence,
    )


def test_report_repr_does_not_dump_the_whole_trace() -> None:
    # The report is reachable from a failing test's traceback, and it now
    # carries every event of the failing run for the timeline to draw. A
    # dataclass repr would print all of them into the terminal.
    def tick() -> None:
        pass

    async def churns_at(bad_seed: int) -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, simloop.SimLoop)
        for _ in range(100):
            loop.call_soon(tick)
            await asyncio.sleep(0)
        if loop.seed == bad_seed:
            raise RuntimeError("boom")

    report = explore(lambda: churns_at(1), range(4), trace_tail=2)
    assert report is not None
    assert len(report.trace) > 200
    text = repr(report)
    assert len(text) < 1_000
    assert "trace=" not in text
    assert "trace_events=" in text


def test_sim_test_reraises_with_report_note() -> None:
    @sim_test(seeds=10)
    async def my_test() -> None:
        await _fails_at(3)

    with pytest.raises(RuntimeError) as excinfo:
        my_test()
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("simloop: failed at seed 3" in note for note in notes)


def test_sim_test_passes_quietly() -> None:
    @sim_test(seeds=5)
    async def my_test() -> None:
        await _fails_at(99)

    my_test()  # must simply return


def test_sim_test_bare_form_defaults_to_ten_seeds() -> None:
    ran: list[int] = []

    @sim_test
    async def my_test() -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, simloop.SimLoop)
        ran.append(loop.seed)

    my_test()
    assert ran == list(range(10))


def test_sim_test_respects_replay_override() -> None:
    from simloop._explore import overrides

    ran: list[int] = []

    @sim_test(seeds=5)
    async def my_test() -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, simloop.SimLoop)
        ran.append(loop.seed)

    overrides.replay = 42
    try:
        my_test()
    finally:
        overrides.replay = None
    assert ran == [42]


def test_sim_test_respects_seed_count_override() -> None:
    from simloop._explore import overrides

    ran: list[int] = []

    @sim_test(seeds=2)
    async def my_test() -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, simloop.SimLoop)
        ran.append(loop.seed)

    overrides.seeds = 4
    try:
        my_test()
    finally:
        overrides.seeds = None
    assert ran == list(range(4))


def test_sim_test_rejects_empty_seed_set() -> None:
    with pytest.raises(ValueError):

        @sim_test(seeds=0)
        async def my_test() -> None:
            pass


# ----------------------------------------------------------------------
# Priority-based exploration
# ----------------------------------------------------------------------
#
# Measured on the race below, five disjoint blocks of 200 seeds (0-199,
# 200-399, ... 800-999): mean seeds to the first failing seed was 2.0 under
# policy="random" (blocks 1, 1, 1, 2, 5) and 92.75 under policy="pct"
# (blocks 9, 26, 146, 190 — block one is excluded because its first failure
# was seed 0, which always runs the seeded schedule to size the horizon and
# so says nothing about PCT); per-seed failure rate 474/1000 against
# 21/1000. Uniform draws win here and it is not close. The reason is not that PCT is doing
# something wrong: this race is two ordering constraints deep in a twelve-step
# run, which is the shape a uniform draw is already ideal for, while PCT runs
# one chain to completion — the schedule that passes — except where a change
# point falls, and its change points are spread over a horizon floored at 100
# steps. Dropping that floor to the measured 18 steps narrows the gap to
# 103/1000 and does not change the verdict. What PCT buys on this workload is
# not a better hit rate but a floor under it: a stated per-run probability of
# hitting a depth-3 bug, which uniform draws cannot promise at any depth.


async def _bump(cell: list[int]) -> None:
    """Read the counter, yield, then write back one more than it read."""
    seen = cell[0]
    await asyncio.sleep(0)
    cell[0] = seen + 1


async def _lost_update() -> None:
    """The canonical depth-2 race: two read-yield-write bumps of one counter.

    The assertion runs in a third task — the one ``run_until_complete`` wraps
    this coroutine in — after both bumps have finished. The counter reaches 2
    only when one bump's write lands before the other's read.
    """
    loop = asyncio.get_running_loop()
    cell = [0]
    bumps = [loop.create_task(_bump(cell)) for _ in range(2)]
    await asyncio.gather(*bumps)
    assert cell[0] == 2, f"lost update: counter is {cell[0]}"


def _run_race(
    seed: int, policy: SchedulingPolicy | None = None
) -> tuple[Exception | None, str]:
    """Run the race once at ``seed``, optionally under ``policy``."""
    loop = simloop.SimLoop(seed)
    if policy is not None:
        loop._policy = policy
    try:
        failure = run_once(loop, _lost_update)
        return failure, loop.trace_hash()
    finally:
        finish(loop)


def test_pct_finds_the_race_and_names_the_policy() -> None:
    report = explore(lambda: _lost_update(), range(1, 100), policy="pct")
    assert report is not None
    assert isinstance(report.exception, AssertionError)
    assert "lost update" in str(report.exception)
    assert report.seeds_passed == report.seed - 1  # every seed below it passed
    assert report.policy == PolicyRun("pct", depth=3, horizon=100)
    assert "policy: pct (depth 3, horizon 100)" in report.render()


def test_pct_schedules_a_seed_differently_from_the_seeded_draw() -> None:
    report = explore(lambda: _lost_update(), range(1, 100), policy="pct")
    assert report is not None and report.policy is not None
    horizon = report.policy.horizon
    assert horizon is not None
    failure, pct_hash = _run_race(report.seed, PCTPolicy(report.seed, 3, horizon))
    # The explorer ran exactly this policy: same failure, same trace.
    assert failure is not None
    assert pct_hash == report.trace_hash
    assert _run_race(report.seed)[1] != pct_hash


def test_a_pct_failure_replays_at_its_own_seed() -> None:
    found = explore(lambda: _lost_update(), range(1, 100), policy="pct")
    assert found is not None
    # What --simloop-replay does: the one seed, the same options. The horizon
    # is measured off seed 0 whatever seeds were asked for, so the replay runs
    # the schedule that found the failure rather than a differently sized one.
    again = explore(lambda: _lost_update(), [found.seed], policy="pct")
    assert again is not None
    assert again.seed == found.seed
    assert again.seeds_passed == 0
    assert again.trace_hash == found.trace_hash
    assert str(again.exception) == str(found.exception)
    assert again.policy == found.policy


def test_a_pct_run_records_choices_that_replay_under_the_scripted_policy() -> None:
    found = explore(lambda: _lost_update(), range(1, 100), policy="pct")
    assert found is not None and found.policy is not None
    horizon = found.policy.horizon
    assert horizon is not None
    loop = simloop.SimLoop(found.seed)
    loop._policy = PCTPolicy(found.seed, found.policy.depth, horizon)
    try:
        failure = run_once(loop, _lost_update)
        choices = loop._choices
        recorded_hash = loop.trace_hash()
    finally:
        finish(loop)
    assert failure is not None
    assert len(choices) > 0  # PCT runs record their choices like any other run
    replay = simloop.SimLoop._from_choices(choices, found.seed)
    try:
        repeated = run_once(replay, _lost_update)
        replay_hash = replay.trace_hash()
    finally:
        finish(replay)
    assert repeated is not None
    assert type(repeated) is type(failure)
    assert str(repeated) == str(failure)
    assert replay_hash == recorded_hash
    assert replay._diverged_at is None


def test_seed_zero_sizes_the_horizon_under_the_default_schedule() -> None:
    # Seed 0 is what the horizon is measured from, so it runs the seeded
    # schedule under every policy — and it explores like any other seed.
    report = explore(lambda: _lost_update(), range(4), policy="pct")
    assert report is not None
    assert report.seed == 0
    assert report.policy == PolicyRun("pct", depth=3, horizon=None)
    assert report.trace_hash == _run_race(0)[1]
    assert "policy: pct (depth 3); this seed ran the default schedule" in (
        report.render()
    )


def test_random_policy_is_the_default_and_renders_no_policy_line() -> None:
    default = explore(lambda: _interleaves(3), range(10))
    named = explore(lambda: _interleaves(3), range(10), policy="random")
    assert default is not None and named is not None
    assert default.policy is None and named.policy is None
    assert default.trace_hash == named.trace_hash
    text = default.render("tests/test_demo.py::test_x")
    assert named.render("tests/test_demo.py::test_x") == text
    assert "policy:" not in text
    assert text.splitlines()[1] == (
        "replay: pytest 'tests/test_demo.py::test_x' --simloop-replay=3"
    )


def test_pct_report_replays_with_the_policy_options() -> None:
    report = explore(lambda: _lost_update(), range(1, 100), policy="pct")
    assert report is not None
    line = report.render("tests/test_demo.py::test_x").splitlines()[1]
    assert line == (
        f"replay: pytest 'tests/test_demo.py::test_x' "
        f"--simloop-replay={report.seed} --simloop-policy=pct"
    )
    deep = explore(lambda: _lost_update(), range(1, 100), policy="pct", pct_depth=4)
    assert deep is not None
    assert "--simloop-pct-depth=4" in deep.render("tests/test_demo.py::test_x")


def test_horizon_is_measured_floored_and_left_room_for_the_depth() -> None:
    assert _pct_horizon(1000, 3) == 1500  # step count x 1.5, rounded up
    assert _pct_horizon(1001, 3) == 1502
    assert _pct_horizon(12, 3) == 100  # short runs take the floor
    # A depth deeper than the measured horizon would be a construction error
    # in PCTPolicy, so the horizon makes room for its change points instead.
    assert _pct_horizon(12, 200) == 199
    assert _pct_horizon(12, 101) == 100


def test_a_deep_pct_depth_still_runs() -> None:
    # 200 change points do not fit in the horizon this twelve-step workload
    # measures, and PCTPolicy refuses depth - 1 > horizon: the widened horizon
    # is what keeps a deep search of a short workload from being a crash.
    report = explore(lambda: _lost_update(), range(1, 6), policy="pct", pct_depth=200)
    assert report is not None
    assert report.policy == PolicyRun("pct", 200, 199)


def test_explore_rejects_an_unknown_policy() -> None:
    with pytest.raises(ValueError, match="unknown scheduling policy 'greedy'"):
        explore(lambda: _fails_at(0), range(1), policy="greedy")


def test_explore_rejects_a_pct_depth_below_one() -> None:
    with pytest.raises(ValueError, match="pct_depth must be at least 1"):
        explore(lambda: _fails_at(0), range(1), policy="pct", pct_depth=0)


def test_explore_refuses_pct_across_worker_processes() -> None:
    with pytest.raises(ValueError, match="sequential"):
        explore(lambda: _fails_at(0), range(4), policy="pct", jobs=2)


def test_sim_test_takes_a_policy() -> None:
    @sim_test(seeds=100, policy="pct")
    async def my_test() -> None:
        await _lost_update()

    with pytest.raises(AssertionError) as excinfo:
        my_test()
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("policy: pct (depth 3" in note for note in notes)


def test_sim_test_respects_the_policy_overrides() -> None:
    from simloop._explore import overrides

    @sim_test(seeds=100, policy="pct")
    async def my_test() -> None:
        await _lost_update()

    overrides.policy = "random"
    try:
        with pytest.raises(AssertionError) as excinfo:
            my_test()
    finally:
        overrides.policy = None
    notes = getattr(excinfo.value, "__notes__", [])
    assert not any("policy:" in note for note in notes)

    overrides.policy = "pct"
    overrides.pct_depth = 5
    try:
        with pytest.raises(AssertionError) as deep:
            my_test()
    finally:
        overrides.policy = None
        overrides.pct_depth = None
    deep_notes = getattr(deep.value, "__notes__", [])
    assert any("policy: pct (depth 5" in note for note in deep_notes)


def test_sim_test_rejects_an_unknown_policy() -> None:
    with pytest.raises(ValueError, match="unknown scheduling policy 'greedy'"):

        @sim_test(policy="greedy")
        async def my_test() -> None:
            pass


def test_format_event_names_the_host_when_there_is_one() -> None:
    on_a_host = TraceEvent("run", 1.0, 7, "Worker.step", "node1")
    assert _format_event(on_a_host) == "  [t=1.0000] run      seq=7  node1  Worker.step"
    # An event that belongs to the simulation rather than to a machine keeps
    # the format it always had.
    assert _format_event(TraceEvent("advance", 1.0, -1, "")) == (
        "  [t=1.0000] advance  seq=-1  "
    )


def test_render_attributes_trace_events_to_their_hosts() -> None:
    report = explore(lambda: _leaves_a_pending_task(), range(1), trace_tail=20)
    assert report is not None
    text = report.render()
    # Trace lines only: the pending-task block names hosts of its own.
    hosted = [
        line
        for line in text.splitlines()
        if line.startswith("  [t=") and " node1  " in line
    ]
    assert hosted, text


def test_public_exports() -> None:
    assert simloop.sim_test is sim_test
    assert simloop.SeedReport is SeedReport
    assert simloop.explore is explore


def test_import_simloop_does_not_import_pytest() -> None:
    code = (
        "import simloop, sys; "
        "raise SystemExit(1 if 'pytest' in sys.modules else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stderr
