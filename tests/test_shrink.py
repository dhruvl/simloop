"""Schedule shrinking: reduce a failing run to the choices that caused it."""

import asyncio
from collections.abc import Iterable, Sequence

import pytest

import simloop
from simloop import SeedReport
from simloop._explore import explore
from simloop._run import Workload, finish, run_once
from simloop._shrink import (
    ShrinkResult,
    _candidate,
    _ddmin,
    _fifo_prefix,
    _same_failure,
    _shortest_prefix,
    shrink_schedule,
)

# Steps at the head of the run with a single ready callback, so the recording
# has a stretch that is FIFO whatever the seed drew.
_WARMUP = 6


class _Cell:
    """A counter whose read-modify-write is split across two callbacks."""

    def __init__(self) -> None:
        self.value = 0


def _bump(cell: _Cell) -> None:
    cell.value += 1


def _read(cell: _Cell, loop: asyncio.AbstractEventLoop) -> None:
    loop.call_soon(_write, cell, cell.value)


def _write(cell: _Cell, seen: int) -> None:
    cell.value = seen + 1


async def _lost_update() -> None:
    """One scheduling choice decides whether a write survives.

    ``_bump`` is scheduled first, so a scheduler that takes the ready queue
    in order runs it before ``_read`` samples the cell, and both increments
    land. Running ``_read`` first instead samples the pre-bump value, and
    the write it schedules overwrites the bump. Nothing else in the run is
    order-sensitive, so the minimal failing schedule is FIFO with exactly
    one recorded choice left in it.
    """
    loop = asyncio.get_running_loop()
    cell = _Cell()
    for _ in range(_WARMUP):
        await asyncio.sleep(0)
    loop.call_soon(_bump, cell)
    loop.call_soon(_read, cell, loop)
    await asyncio.sleep(0.01)
    # Read into a local first: an assertion rewritten against the object
    # would put its repr, and with it a memory address, into the message the
    # shrinker matches on.
    total = cell.value
    assert total == 2, "lost update"


def _first_failure(
    fn: Workload, seeds: Iterable[int] = range(10)
) -> tuple[int, tuple[int, ...], Exception]:
    """Run seeds until one fails; report its seed, choices and exception."""
    for seed in seeds:
        loop = simloop.SimLoop(seed)
        try:
            failure = run_once(loop, fn)
            if failure is not None:
                return seed, loop._choices, failure
        finally:
            finish(loop)
    raise AssertionError("no seed failed")


def test_fifo_alone_keeps_both_updates() -> None:
    # The hand-computed minimum rests on this: a schedule with no recorded
    # choices left in it passes, so the shrinker cannot do better than one.
    loop = simloop.SimLoop._from_choices((), 0)
    try:
        assert run_once(loop, _lost_update) is None
    finally:
        finish(loop)


def test_shrinks_a_lost_update_to_a_single_choice() -> None:
    seed, choices, failure = _first_failure(_lost_update)
    result = shrink_schedule(_lost_update, seed, choices, failure)
    assert result is not None
    assert result.original_len == len(choices)
    assert len(result.kept) == 1
    assert result.fifo_prefix == result.kept[0]
    assert result.fifo_prefix >= _WARMUP
    assert result.labels == ("_read",)
    assert len(result.choices) < len(choices)
    assert result.oracle_runs > 0


def test_the_shrunk_schedule_still_fails_the_same_way() -> None:
    seed, choices, failure = _first_failure(_lost_update)
    result = shrink_schedule(_lost_update, seed, choices, failure)
    assert result is not None
    loop = simloop.SimLoop._from_choices(result.choices, seed)
    try:
        assert _same_failure(failure, run_once(loop, _lost_update))
    finally:
        finish(loop)


def test_shrinking_stops_at_its_budget_and_keeps_the_best_so_far() -> None:
    seed, choices, failure = _first_failure(_lost_update)
    result = shrink_schedule(_lost_update, seed, choices, failure, budget=2)
    assert result is not None
    assert result.oracle_runs <= 2
    # Too little budget to minimize anything, so the answer is the recording
    # itself: still a failing schedule, just not a reduced one.
    assert len(result.kept) >= 1
    loop = simloop.SimLoop._from_choices(result.choices, seed)
    try:
        assert _same_failure(failure, run_once(loop, _lost_update))
    finally:
        finish(loop)


def test_shrinking_rejects_a_budget_below_one() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        shrink_schedule(_lost_update, 0, (0,), RuntimeError("boom"), budget=0)


def test_a_recording_that_does_not_reproduce_shrinks_to_nothing() -> None:
    seed, choices, _ = _first_failure(_lost_update)
    other = ValueError("a failure this run never produces")
    assert shrink_schedule(_lost_update, seed, choices, other) is None


# ----------------------------------------------------------------------
# Oracle
# ----------------------------------------------------------------------


def test_same_type_and_message_is_the_same_failure() -> None:
    assert _same_failure(RuntimeError("boom"), RuntimeError("boom"))


def test_a_run_that_stopped_failing_is_not_the_same_failure() -> None:
    assert not _same_failure(RuntimeError("boom"), None)


def test_a_different_exception_type_is_not_the_same_failure() -> None:
    assert not _same_failure(RuntimeError("boom"), ValueError("boom"))


def test_a_subclass_is_not_the_same_failure() -> None:
    class Subclass(RuntimeError):
        pass

    assert not _same_failure(RuntimeError("boom"), Subclass("boom"))


def test_a_different_message_is_not_the_same_failure() -> None:
    assert not _same_failure(AssertionError("lost update"), AssertionError("deadlock"))


def test_messages_may_differ_past_the_compared_prefix() -> None:
    shared = "lost update in the ledger " + "x" * 60
    assert len(shared) > 80
    assert _same_failure(
        AssertionError(shared + " for worker 3"),
        AssertionError(shared + " for worker 9"),
    )


def test_messages_that_differ_within_the_prefix_are_distinct() -> None:
    assert not _same_failure(
        AssertionError("lost update at step 1" + "x" * 100),
        AssertionError("lost update at step 2" + "x" * 100),
    )


# ----------------------------------------------------------------------
# Reduction passes, driven by a synthetic oracle
# ----------------------------------------------------------------------

_RECORDED = (3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8)


def test_shortest_prefix_finds_the_smallest_failing_truncation() -> None:
    seen: list[int] = []

    def probe(candidate: Sequence[int]) -> bool:
        seen.append(len(candidate))
        return len(candidate) >= 7

    assert _shortest_prefix(_RECORDED, probe) == 7
    # A linear scan would cost one run per step; a binary search must not.
    assert len(seen) <= 5


def test_shortest_prefix_truncates_everything_when_fifo_fails() -> None:
    assert _shortest_prefix(_RECORDED, lambda candidate: True) == 0


def test_fifo_prefix_finds_the_largest_leading_run_of_fifo() -> None:
    def probe(candidate: Sequence[int]) -> bool:
        # Fails only while step 5 still holds its recorded choice.
        return len(candidate) > 5 and candidate[5] == _RECORDED[5]

    assert _fifo_prefix(_RECORDED, len(_RECORDED), probe) == 5


def test_fifo_prefix_is_the_whole_recording_when_fifo_fails() -> None:
    assert (
        _fifo_prefix(_RECORDED, len(_RECORDED), lambda candidate: True)
        == len(_RECORDED)
    )


def test_ddmin_keeps_only_the_positions_the_failure_needs() -> None:
    needed = (2, 9)

    def probe(candidate: Sequence[int]) -> bool:
        return all(candidate[position] == _RECORDED[position] for position in needed)

    kept = _ddmin(_RECORDED, len(_RECORDED), range(len(_RECORDED)), probe)
    assert kept == needed


def test_ddmin_keeps_a_lone_position() -> None:
    def probe(candidate: Sequence[int]) -> bool:
        return candidate[7] == _RECORDED[7]

    assert _ddmin(_RECORDED, len(_RECORDED), range(len(_RECORDED)), probe) == (7,)


def test_ddmin_keeps_everything_when_every_position_matters() -> None:
    positions = tuple(range(len(_RECORDED)))

    def probe(candidate: Sequence[int]) -> bool:
        return all(candidate[position] == _RECORDED[position] for position in positions)

    assert _ddmin(_RECORDED, len(_RECORDED), positions, probe) == positions


def test_candidate_reverts_every_position_it_is_not_given() -> None:
    assert _candidate(_RECORDED, 5, (1, 3)) == (0, 1, 0, 1, 0)


# ----------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------


def test_explore_leaves_the_report_unshrunk_by_default() -> None:
    report = explore(_lost_update, range(10))
    assert report is not None
    assert report.shrunk is None
    assert "shrink" not in report.render()


def test_explore_shrinks_on_request() -> None:
    report = explore(_lost_update, range(10), shrink=True)
    assert report is not None
    assert report.shrunk is not None
    assert len(report.shrunk.kept) == 1


def test_render_shows_the_shrink_block() -> None:
    report = explore(_lost_update, range(10), shrink=True)
    assert report is not None
    assert report.shrunk is not None
    lines = report.render().splitlines()
    step = report.shrunk.kept[0]
    assert any("schedule shrink (experimental):" in line for line in lines)
    assert f"minimized: FIFO except step {step:,}" in lines
    assert f"  step {step:,}  _read" in lines


def test_render_reports_a_schedule_that_did_not_need_a_choice() -> None:
    report = explore(_always_fails, range(1), shrink=True)
    assert report is not None
    assert report.shrunk is not None
    assert report.shrunk.kept == ()
    assert report.shrunk.choices == ()
    assert "minimized: FIFO throughout" in report.render().splitlines()


async def _always_fails() -> None:
    await asyncio.sleep(0.01)
    raise RuntimeError("boom")


def _report_with(shrunk: ShrinkResult) -> SeedReport:
    return SeedReport(
        seed=41,
        seeds_passed=41,
        exception=RuntimeError("boom"),
        trace_events=(),
        trace_hash="0" * 64,
        pending=(),
        shrunk=shrunk,
    )


def test_render_summarizes_a_window_too_wide_to_list() -> None:
    kept = tuple(range(11_201, 11_216))
    shrunk = ShrinkResult(
        original_len=12_403,
        choices=(0,) * 11_216,
        fifo_prefix=kept[0],
        kept=kept,
        labels=("Broker.expire",) * len(kept),
        oracle_runs=500,
    )
    lines = _report_with(shrunk).render().splitlines()
    assert (
        "schedule shrink (experimental): 12,403 steps recorded, "
        "500 runs to minimize"
    ) in lines
    assert "minimized: FIFO except steps 11,201-11,215" in lines
    assert "  step 11,201  Broker.expire" in lines
    assert "  ... and 3 more" in lines


def test_explore_rejects_a_budget_below_one() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        explore(_always_fails, range(1), shrink=True, shrink_budget=0)


# ----------------------------------------------------------------------
# A busier workload, where the window is not known in advance
# ----------------------------------------------------------------------

_WORKERS = ("alice", "bob", "carol")
_ROUNDS = 8


def _noop() -> None:
    pass


async def _busy_ledger() -> None:
    """One unprotected read-modify-write, buried in unrelated work.

    The workers, their queue and their timers never touch the cell, so none
    of them can lose the update on their own. They are here to keep the
    ready queue full, so that the one choice that decides the outcome is
    buried in a few hundred choices that do not.
    """
    loop = asyncio.get_running_loop()
    cell = _Cell()
    inbox: asyncio.Queue[str] = asyncio.Queue()

    async def worker(name: str) -> None:
        for round_ in range(_ROUNDS):
            await inbox.put(f"{name}:{round_}")
            loop.call_later(0.001 * (round_ + 1), _noop)
            await asyncio.sleep(0.001)

    async def collect(total: int) -> None:
        for _ in range(total):
            await inbox.get()

    collector = asyncio.create_task(collect(len(_WORKERS) * _ROUNDS))
    workers = [asyncio.create_task(worker(name)) for name in _WORKERS]
    await asyncio.sleep(0.002)
    loop.call_soon(_bump, cell)
    loop.call_soon(_read, cell, loop)
    for task in workers:
        await task
    await collector
    await asyncio.sleep(0.05)
    total = cell.value
    assert total == 2, "lost update"


def test_a_busier_workload_shrinks_to_a_narrow_window() -> None:
    seed, choices, failure = _first_failure(_busy_ledger, range(30))
    result = shrink_schedule(_busy_ledger, seed, choices, failure)
    assert result is not None
    assert len(choices) > 100
    # The recording is mostly noise: whatever window survives here is a
    # small fraction of it, and the labels name what ran in that window.
    assert len(result.kept) <= 5
    assert result.fifo_prefix > 0
    assert len(result.labels) == len(result.kept)
    assert result.oracle_runs <= 500
    loop = simloop.SimLoop._from_choices(result.choices, seed)
    try:
        assert _same_failure(failure, run_once(loop, _busy_ledger))
    finally:
        finish(loop)
