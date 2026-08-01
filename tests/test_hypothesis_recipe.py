"""Hypothesis over the data, simloop over the schedule.

The two searches are meant to compose: Hypothesis picks the workload, simloop
picks the interleaving that workload runs under. This file is that claim as a
test rather than a sentence — a correct log that survives every generated
workload under every seed, and a racy one where the combination finds the bug,
shrinks the *workload* to its minimum, and hands back a seed that replays the
*schedule* on its own.

Seeds are deliberately not a Hypothesis strategy: see docs/cookbook.md for why
the two shrinkers must not be pointed at the same failure.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from simloop import explore, sim_test

# Seeds each generated workload is explored under. Small on purpose: this runs
# once per example, so the file's cost is examples x seeds, and a race two
# tasks deep does not need a long search to turn up.
SEEDS = 8

# Payload alphabet and sizes kept tiny — they exist to be shrunk, not to
# exercise string handling.
_PAYLOADS = st.lists(
    st.text(alphabet="abc", min_size=1, max_size=3), min_size=1, max_size=3
)
_WRITERS = st.integers(min_value=1, max_value=4)
# How long a write is in flight, in virtual seconds. 0.0 means "yield once",
# where the interleaving is the seed's to choose; the longer draws park every
# writer on a timer, which is a different shape of workload and costs nothing
# in wall time.
_DELAYS = st.sampled_from((0.0, 0.001, 0.010))

# Every Hypothesis test here shares one configuration. `deadline=None` because
# a simulated run's wall-clock duration is meaningless — it compresses whatever
# virtual time the workload asks for, so a per-example time limit measures the
# machine and not the code. `derandomize` and `database=None` are what make CI
# reproducible: the examples come from a hash of the test rather than from the
# clock, and no example from a previous run is replayed out of `.hypothesis/`.
_SETTINGS = settings(
    deadline=None, derandomize=True, database=None, max_examples=25
)


async def _reserve_then_write(log: list[str], payload: str, delay: float) -> None:
    """Append to ``log`` by reserving an index and then writing to it.

    The gap between the two is the whole bug: a writer that reserves an index
    while another writer holds the same one overwrites its append instead of
    adding to it.
    """
    index = len(log)
    await asyncio.sleep(delay)
    log[index : index + 1] = [payload]


async def _write_batch(
    log: list[str],
    lock: asyncio.Lock,
    payloads: Sequence[str],
    delay: float,
    guarded: bool,
) -> None:
    for payload in payloads:
        if guarded:
            async with lock:
                await _reserve_then_write(log, payload, delay)
        else:
            await _reserve_then_write(log, payload, delay)


async def _replicate(
    writers: int, payloads: Sequence[str], delay: float, *, guarded: bool
) -> None:
    """Concurrent writers appending their payloads to one shared log.

    The invariant is the only thing a caller cares about: every append that was
    started is in the log. ``guarded`` is the fix — holding the lock across the
    reserve and the write — and the two versions run the same appending code.
    """
    loop = asyncio.get_running_loop()
    lock = asyncio.Lock()
    log: list[str] = []
    batches = [
        loop.create_task(_write_batch(log, lock, payloads, delay, guarded))
        for _ in range(writers)
    ]
    await asyncio.gather(*batches)
    expected = writers * len(payloads)
    assert len(log) == expected, f"lost {expected - len(log)} of {expected} appends"


@_SETTINGS
@given(writers=_WRITERS, payloads=_PAYLOADS, delay=_DELAYS)
def test_the_locked_log_holds_across_examples_and_seeds(
    writers: int, payloads: list[str], delay: float
) -> None:
    # The recipe itself: Hypothesis names a workload, explore() runs it under
    # every seed, and the report — not an exception — is what says whether any
    # schedule broke it.
    report = explore(
        lambda: _replicate(writers, payloads, delay, guarded=True), range(SEEDS)
    )
    assert report is None, report.render()


def test_hypothesis_shrinks_the_workload_while_the_seed_pins_the_schedule() -> None:
    # Hypothesis reports its minimal falsifying example by running it one last
    # time, so the last workload recorded here is the one it settled on.
    failed: list[tuple[int, list[str], float, int, str]] = []

    @_SETTINGS
    @given(writers=_WRITERS, payloads=_PAYLOADS, delay=_DELAYS)
    def every_append_survives(
        writers: int, payloads: list[str], delay: float
    ) -> None:
        report = explore(
            lambda: _replicate(writers, payloads, delay, guarded=False),
            range(SEEDS),
        )
        if report is not None:
            failed.append(
                (writers, payloads, delay, report.seed, report.trace_hash)
            )
            raise AssertionError(report.render())

    with pytest.raises(AssertionError):
        every_append_survives()

    writers, payloads, delay, seed, trace_hash = failed[-1]
    # Two writers, one payload each, and a write that merely yields: the
    # smallest workload that can lose an append. Hypothesis got there by
    # shrinking the data — nothing about the schedule was shrunk, and the
    # seeds it explored were the same range(SEEDS) every time.
    assert (writers, payloads, delay) == (2, ["a"], 0.0)
    # Bigger workloads failed on the way here — more writers, longer payload
    # lists, writes that sit on a timer — and the reported example is the
    # smallest of everything the search saw fail.
    assert any(entry[0] > 2 or len(entry[1]) > 1 or entry[2] > 0.0 for entry in failed)
    assert min(entry[:3] for entry in failed) == (writers, payloads, delay)

    # And the schedule half of the report stands on its own: the seed alone
    # reproduces the failure, with the same trace, without the seeds around it.
    replay = explore(
        lambda: _replicate(writers, payloads, delay, guarded=False), [seed]
    )
    assert replay is not None
    assert replay.seed == seed
    assert replay.seeds_passed == 0
    assert replay.trace_hash == trace_hash
    assert isinstance(replay.exception, AssertionError)
    assert "lost 1 of 2 appends" in str(replay.exception)


@_SETTINGS
@given(writers=_WRITERS, payloads=_PAYLOADS, delay=_DELAYS)
@sim_test(seeds=SEEDS)
async def test_the_decorators_stack_into_the_short_form(
    writers: int, payloads: list[str], delay: float
) -> None:
    # The same recipe written the short way: @sim_test turns the coroutine
    # into a synchronous test that explores seeds, and @given calls that test
    # once per example. It works because the wrapper passes its arguments
    # through to the coroutine — one example, one exploration.
    await _replicate(writers, payloads, delay, guarded=True)


def test_the_same_workload_and_seed_pin_one_schedule() -> None:
    # What lets Hypothesis shrink at all: for a fixed workload, the seed range
    # is a pure function, so a workload that failed and then shrank cannot have
    # changed its verdict for a reason the search cannot see.
    def run() -> str:
        report = explore(
            lambda: _replicate(2, ["a"], 0.0, guarded=False), range(SEEDS)
        )
        assert report is not None
        return f"{report.seed}:{report.trace_hash}"

    assert run() == run()
