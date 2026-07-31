"""Parallel seed exploration: the same answer as sequential, on more cores."""

from __future__ import annotations

import asyncio
import functools
import multiprocessing
import subprocess
import sys

import pytest

import simloop
from simloop._explore import explore
from simloop._parallel import (
    BATCH_SIZE,
    Failure,
    ParallelDeterminismError,
    _batch_size,
    _batches,
    _Frontier,
    check_reproduced,
    find_lowest_failure,
    require_picklable,
)


def _failure(at: int) -> Failure:
    return Failure(at, "RuntimeError", "boom")


# ----------------------------------------------------------------------
# Batch frontier
# ----------------------------------------------------------------------


def test_batches_cover_every_position_in_ascending_order() -> None:
    assert [tuple(batch) for batch in _batches(10, 4)] == [
        (0, 1, 2, 3),
        (4, 5, 6, 7),
        (8, 9),
    ]
    assert _batches(0, 4) == []


def test_batch_size_gives_every_worker_several_turns() -> None:
    assert _batch_size(1600, 4) == BATCH_SIZE
    assert _batch_size(200, 10) == 5
    assert _batch_size(6, 4) == 1


def test_frontier_hands_out_every_batch_when_all_of_them_pass() -> None:
    frontier = _Frontier(_batches(48, 16))
    for _ in range(3):
        batch = frontier.take()
        assert batch is not None
        frontier.record(None)
    assert frontier.take() is None
    assert frontier.failure is None


def test_frontier_stops_handing_out_work_after_the_first_batch_fails() -> None:
    frontier = _Frontier(_batches(64, 16))
    assert frontier.take() == range(0, 16)
    frontier.record(_failure(3))
    assert frontier.take() is None
    assert frontier.failure == _failure(3)


def test_frontier_keeps_the_lowest_failure_whatever_order_they_arrive() -> None:
    # Four batches out at once; the last one to report holds the lowest seed.
    frontier = _Frontier(_batches(64, 16))
    for _ in range(4):
        assert frontier.take() is not None
    frontier.record(_failure(35))
    frontier.record(None)
    frontier.record(_failure(5))
    frontier.record(_failure(60))
    assert frontier.failure == _failure(5)


def test_frontier_prefers_the_lower_failure_reported_second() -> None:
    frontier = _Frontier(_batches(64, 16))
    assert frontier.take() is not None
    assert frontier.take() is not None
    frontier.record(_failure(20))
    # A batch already running can still beat a failure that is on the board,
    # which is why the frontier keeps a minimum rather than the first answer.
    frontier.record(_failure(3))
    assert frontier.failure == _failure(3)


def test_frontier_hands_out_nothing_beyond_the_last_batch() -> None:
    frontier = _Frontier(_batches(5, 16))
    assert frontier.take() == range(0, 5)
    assert frontier.take() is None


# ----------------------------------------------------------------------
# Workload picklability
# ----------------------------------------------------------------------


async def _fails_at(bad_seed: int) -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    await asyncio.sleep(1.0)
    if loop.seed == bad_seed:
        raise RuntimeError("boom")


async def _fails_at_any(bad_seeds: tuple[int, ...]) -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    await asyncio.sleep(1.0)
    if loop.seed in bad_seeds:
        raise RuntimeError("boom")


def test_require_picklable_accepts_a_module_level_partial() -> None:
    require_picklable(functools.partial(_fails_at, 3))


def test_require_picklable_names_the_constraint_for_a_lambda() -> None:
    with pytest.raises(TypeError) as excinfo:
        require_picklable(lambda: _fails_at(3))
    message = str(excinfo.value)
    assert "picklable" in message
    assert "Lambdas" in message


def test_explore_rejects_an_unpicklable_workload_before_running_anything() -> None:
    with pytest.raises(TypeError, match="picklable"):
        explore(lambda: _fails_at(3), range(32), jobs=2)


def test_explore_rejects_a_job_count_below_one() -> None:
    with pytest.raises(ValueError, match="jobs"):
        explore(functools.partial(_fails_at, 3), range(4), jobs=0)


# ----------------------------------------------------------------------
# Cross-process reproduction check
# ----------------------------------------------------------------------


def test_check_passes_when_both_processes_saw_the_same_failure() -> None:
    check_reproduced(3, _failure(3), RuntimeError("boom"))


def test_check_passes_when_both_processes_saw_the_seed_pass() -> None:
    check_reproduced(3, None, None)


def test_check_reports_a_failure_the_parent_did_not_reproduce() -> None:
    with pytest.raises(ParallelDeterminismError) as excinfo:
        check_reproduced(7, _failure(7), None)
    message = str(excinfo.value)
    assert "seed 7" in message
    assert "raised RuntimeError" in message
    assert "passed" in message


def test_check_reports_a_different_exception_type() -> None:
    with pytest.raises(ParallelDeterminismError, match="ValueError"):
        check_reproduced(7, _failure(7), ValueError("boom"))


def test_check_reports_a_seed_that_only_failed_in_the_parent() -> None:
    with pytest.raises(ParallelDeterminismError, match="seed 6"):
        check_reproduced(6, None, RuntimeError("boom"))


# ----------------------------------------------------------------------
# End to end
# ----------------------------------------------------------------------


def test_parallel_search_reports_the_lowest_failing_seed_not_the_first_found() -> None:
    # One seed per batch across four workers, so seed 40 is under way — and
    # very likely finished — before seed 5 has reported.
    found = find_lowest_failure(
        functools.partial(_fails_at_any, (5, 40)),
        list(range(48)),
        jobs=4,
        batch_size=1,
    )
    assert found is not None
    assert found.at == 5
    assert found.exc_type == "RuntimeError"


def test_parallel_explore_matches_sequential_seed_for_seed() -> None:
    workload = functools.partial(_fails_at, 37)
    expected = explore(workload, range(64))
    actual = explore(workload, range(64), jobs=2)
    assert expected is not None and actual is not None
    assert actual.seed == expected.seed == 37
    assert actual.seeds_passed == expected.seeds_passed == 37
    assert actual.trace_hash == expected.trace_hash
    assert actual.trace_events == expected.trace_events
    assert actual.divergence == expected.divergence
    assert type(actual.exception) is type(expected.exception)
    assert str(actual.exception) == str(expected.exception)


def test_parallel_explore_returns_none_when_every_seed_passes() -> None:
    assert explore(functools.partial(_fails_at, 999), range(64), jobs=2) is None


def test_parallel_explore_finds_a_failure_in_the_first_batch() -> None:
    report = explore(functools.partial(_fails_at, 0), range(64), jobs=2)
    assert report is not None
    assert report.seed == 0
    assert report.seeds_passed == 0
    assert report.divergence is None


def test_parallel_explore_shrinks_the_same_schedule_as_sequential() -> None:
    workload = functools.partial(_fails_at, 20)
    expected = explore(workload, range(32), shrink=True)
    actual = explore(workload, range(32), shrink=True, jobs=2)
    assert expected is not None and actual is not None
    assert actual.shrunk == expected.shrunk


def test_one_seed_needs_no_worker_processes() -> None:
    report = explore(functools.partial(_fails_at, 4), [4], jobs=4)
    assert report is not None
    assert report.seed == 4


async def _fails_only_in_workers() -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, simloop.SimLoop)
    await asyncio.sleep(1.0)
    if multiprocessing.parent_process() is not None:
        raise RuntimeError("boom")


def test_a_failure_the_parent_cannot_reproduce_is_reported_loudly() -> None:
    with pytest.raises(ParallelDeterminismError) as excinfo:
        explore(_fails_only_in_workers, range(32), jobs=2)
    assert "seed 0" in str(excinfo.value)
    assert "jobs=1" in str(excinfo.value)


def test_import_simloop_does_not_import_multiprocessing() -> None:
    # The pool machinery is imported where it is used, so that a sequential
    # run pays nothing for it.
    code = (
        "import simloop, sys; "
        "raise SystemExit(1 if 'multiprocessing' in sys.modules else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stderr
