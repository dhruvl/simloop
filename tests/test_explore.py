"""Explorer core: first-failure seed search over fresh SimLoops."""

import asyncio
import subprocess
import sys

import pytest

import simloop
from simloop import SeedReport, sim_test
from simloop._explore import explore


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
