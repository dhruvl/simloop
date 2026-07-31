"""Explorer core: first-failure seed search over fresh SimLoops."""

import asyncio
import subprocess
import sys

import pytest

import simloop
from simloop import SeedReport, TraceEvent, sim_test
from simloop._explore import Divergence, _diff_traces, explore
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
