"""Explorer core: first-failure seed search over fresh SimLoops."""

import asyncio

import pytest

import simloop
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
