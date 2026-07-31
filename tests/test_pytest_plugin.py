"""Plugin behavior, exercised through real sub-pytest runs."""

import pytest

_FLAKY = """
import asyncio
from simloop import sim_test


@sim_test(seeds=10)
async def test_flaky():
    loop = asyncio.get_running_loop()
    await asyncio.sleep(1.0)
    assert loop.seed != 3
"""


def test_failure_report_names_seed_and_replay_command(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*simloop: failed at seed 3 (3 seeds passed first)*",
            "*replay: pytest 'test_demo.py::test_flaky' --simloop-replay=3*",
        ]
    )


_DIVERGING = """
import asyncio
from simloop import sim_test


def tick_a():
    pass


def tick_b():
    pass


@sim_test(seeds=10)
async def test_flaky():
    loop = asyncio.get_running_loop()
    for _ in range(3):
        await asyncio.sleep(0)
    for _ in range(6):
        loop.call_soon(tick_a)
        loop.call_soon(tick_b)
        await asyncio.sleep(0)
    assert loop.seed != 3
"""


def test_failure_report_diffs_against_the_last_passing_seed(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_demo=_DIVERGING)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*runs agree for * events; passing then ran *, failing ran *",
            "*passing run:*",
            "*failing run:*",
        ]
    )


_LOST_UPDATE = """
import asyncio
from simloop import sim_test


def bump(cell):
    cell[0] += 1


def read(cell, loop):
    loop.call_soon(write, cell, cell[0])


def write(cell, seen):
    cell[0] = seen + 1


@sim_test(seeds=10)
async def test_flaky():
    loop = asyncio.get_running_loop()
    cell = [0]
    loop.call_soon(bump, cell)
    loop.call_soon(read, cell, loop)
    await asyncio.sleep(0.01)
    total = cell[0]
    assert total == 2, "lost update"
"""


def test_shrink_flag_adds_a_minimized_schedule(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_LOST_UPDATE)
    result = pytester.runpytest_subprocess("--simloop-shrink")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*schedule shrink (experimental): * steps recorded, * to minimize*",
            "*minimized: FIFO except step *",
            "*  step *  read*",
        ]
    )


def test_no_shrink_block_by_default(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_LOST_UPDATE)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result.stdout.no_fnmatch_line("*schedule shrink*")


def test_shrink_budget_flag_caps_the_search(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_LOST_UPDATE)
    result = pytester.runpytest_subprocess(
        "--simloop-shrink", "--simloop-shrink-budget=1"
    )
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*steps recorded, 1 run to minimize*"])


def test_shrink_budget_flag_rejects_a_budget_below_one(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_demo=_LOST_UPDATE)
    result = pytester.runpytest_subprocess("--simloop-shrink-budget=0")
    assert result.ret != 0
    result.stderr.fnmatch_lines(["*--simloop-shrink-budget must be at least 1*"])


def test_replay_flag_runs_exactly_one_seed(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-replay=3")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        ["*simloop: failed at seed 3 (0 seeds passed first)*"]
    )
    result = pytester.runpytest_subprocess("--simloop-replay=4")
    result.assert_outcomes(passed=1)


def test_seeds_flag_overrides_decorator_count(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


@sim_test(seeds=2)
async def test_flaky():
    loop = asyncio.get_running_loop()
    await asyncio.sleep(1.0)
    assert loop.seed != 3
"""
    )
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(passed=1)
    result = pytester.runpytest_subprocess("--simloop-seeds=10")
    result.assert_outcomes(failed=1)


def test_plugin_is_silent_without_sim_tests(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
def test_plain():
    assert True
"""
    )
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(passed=1)
    # Match "simloop:" (our output prefix), not bare "simloop" — pytest's
    # own header prints "plugins: simloop-<version>" for any installed
    # entry-point plugin, and that must not fail this test.
    result.stdout.no_fnmatch_line("*simloop:*")


def test_summary_counts_tests_and_seeds(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


@sim_test(seeds=7)
async def test_a():
    await asyncio.sleep(0.1)


@sim_test(seeds=5)
async def test_b():
    await asyncio.sleep(0.1)
"""
    )
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(passed=2)
    result.stdout.fnmatch_lines(["*simloop: 2 sim tests, 12 seeds explored*"])


def test_summary_singular_for_one_test(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


@sim_test(seeds=3)
async def test_a():
    await asyncio.sleep(0.1)
"""
    )
    result = pytester.runpytest_subprocess()
    result.stdout.fnmatch_lines(["*simloop: 1 sim test, 3 seeds explored*"])
