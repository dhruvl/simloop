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


def test_jobs_flag_reports_the_seed_a_sequential_run_would(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-jobs=2", "--simloop-seeds=64")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        ["*simloop: failed at seed 3 (3 seeds passed first)*"]
    )


def test_jobs_flag_passes_a_clean_test(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


@sim_test(seeds=40)
async def test_clean():
    await asyncio.sleep(0.1)
"""
    )
    result = pytester.runpytest_subprocess("--simloop-jobs=2")
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*simloop: 1 sim test, 40 seeds explored*"])


_WITH_FIXTURE = """
import asyncio
import pytest
from simloop import sim_test


@pytest.fixture
def bad_seed():
    return 3


@sim_test(seeds=40)
async def test_flaky(bad_seed):
    loop = asyncio.get_running_loop()
    await asyncio.sleep(1.0)
    assert loop.seed != bad_seed
"""


def test_jobs_flag_refuses_a_test_taking_fixtures(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_WITH_FIXTURE)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result = pytester.runpytest_subprocess("--simloop-jobs=2")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*fixtures cannot be rebuilt in a worker process: "
            "run it without --simloop-jobs*"
        ]
    )


_ONLY_IN_A_WORKER = """
import asyncio
import multiprocessing
from simloop import sim_test


@sim_test(seeds=40)
async def test_flaky():
    await asyncio.sleep(1.0)
    assert multiprocessing.parent_process() is None, "ran in a child process"
"""


def test_jobs_flag_really_runs_seeds_in_other_processes(
    pytester: pytest.Pytester,
) -> None:
    # A workload that can tell which process it is in: sequentially it passes,
    # and under --simloop-jobs it fails in the workers and then refuses to
    # reproduce in the parent, which is the loud path for a broken replay.
    pytester.makepyfile(test_demo=_ONLY_IN_A_WORKER)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(passed=1)
    result = pytester.runpytest_subprocess("--simloop-jobs=2")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        ["*replay did not hold across processes for this workload*"]
    )


def test_jobs_flag_rejects_a_job_count_below_one(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-jobs=0")
    assert result.ret != 0
    result.stderr.fnmatch_lines(["*--simloop-jobs must be at least 1*"])


def test_policy_flag_explores_under_pct(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-policy=pct")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*simloop: failed at seed 3 (3 seeds passed first)*",
            "*replay: pytest 'test_demo.py::test_flaky' --simloop-replay=3 "
            "--simloop-policy=pct*",
            "*policy: pct (depth 3, horizon *)*",
        ]
    )


def test_pct_depth_flag_reaches_the_policy(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess(
        "--simloop-policy=pct", "--simloop-pct-depth=5"
    )
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*--simloop-replay=3 --simloop-policy=pct --simloop-pct-depth=5*",
            "*policy: pct (depth 5, horizon *)*",
        ]
    )


def test_no_policy_line_by_default(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result.stdout.no_fnmatch_line("*policy:*")


def test_policy_flag_rejects_an_unknown_policy(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-policy=greedy")
    assert result.ret != 0
    result.stderr.fnmatch_lines(["*--simloop-policy*invalid choice*"])


def test_pct_depth_flag_rejects_a_depth_below_one(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-pct-depth=0")
    assert result.ret != 0
    result.stderr.fnmatch_lines(["*--simloop-pct-depth must be at least 1*"])


def test_pct_refuses_worker_processes(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess("--simloop-policy=pct", "--simloop-jobs=2")
    assert result.ret != 0
    result.stderr.fnmatch_lines(
        ["*--simloop-policy=pct explores sequentially*--simloop-jobs*"]
    )


_TWO_HOSTS = """
import asyncio
from simloop import sim_test


async def wait():
    await asyncio.sleep(0.5)


@sim_test(seeds=10)
async def test_flaky():
    loop = asyncio.get_running_loop()
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    await alpha.create_task(wait())
    await beta.create_task(wait())
    assert loop.seed != 3
"""


def test_timeline_flag_writes_a_page_for_the_failing_seed(
    pytester: pytest.Pytester,
) -> None:
    pytester.makepyfile(test_demo=_TWO_HOSTS)
    result = pytester.runpytest_subprocess("--simloop-timeline")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*timeline: *simloop-timeline-seed3.html*"])
    written = pytester.path / "simloop-timeline-seed3.html"
    page = written.read_text(encoding="utf-8")
    assert page.startswith("<!doctype html>")
    assert 'data-host="alpha"' in page


def test_timeline_flag_takes_a_directory(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_TWO_HOSTS)
    result = pytester.runpytest_subprocess("--simloop-timeline=artifacts/runs")
    result.assert_outcomes(failed=1)
    written = pytester.path / "artifacts" / "runs" / "simloop-timeline-seed3.html"
    assert written.exists()
    result.stdout.fnmatch_lines(["*timeline: *artifacts?runs?simloop-timeline-seed3*"])


def test_timeline_flag_does_not_swallow_a_test_path(pytester: pytest.Pytester) -> None:
    # The directory is optional, so argparse would happily read the next
    # argument as one; a test path is refused rather than quietly running the
    # whole suite and writing the pages into the test tree.
    pytester.makepyfile(test_demo=_FLAKY)
    suite = pytester.path / "suite"
    suite.mkdir()
    (suite / "test_inner.py").write_text("def test_ok():\n    pass\n")
    for swallowed in ("test_demo.py", "suite", "test_demo.py::test_flaky"):
        result = pytester.runpytest_subprocess("--simloop-timeline", swallowed)
        assert result.ret != 0, swallowed
        result.stderr.fnmatch_lines(["*--simloop-timeline=DIR*"])


def test_timeline_flag_accepts_a_directory_holding_no_tests(
    pytester: pytest.Pytester,
) -> None:
    # The guard must not refuse the ordinary case: an artifacts directory the
    # user made earlier, which is a directory and nothing more.
    pytester.makepyfile(test_demo=_TWO_HOSTS)
    (pytester.path / "artifacts").mkdir()
    result = pytester.runpytest_subprocess("--simloop-timeline", "artifacts")
    result.assert_outcomes(failed=1)
    assert (pytester.path / "artifacts" / "simloop-timeline-seed3.html").exists()


def test_a_timeline_that_cannot_be_written_keeps_the_report(
    pytester: pytest.Pytester,
) -> None:
    # A drawing that cannot be saved must not cost the user the failure it was
    # drawing: the report still names the seed and the replay command, and the
    # exception is still the assertion that failed.
    pytester.makepyfile(test_demo=_TWO_HOSTS)
    (pytester.path / "blocked").write_text("not a directory\n")
    result = pytester.runpytest_subprocess("--simloop-timeline=blocked/pages")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*simloop: failed at seed 3 (3 seeds passed first)*",
            "*replay: pytest 'test_demo.py::test_flaky' --simloop-replay=3*",
            "*timeline not written: *blocked*",
            "*last * trace events:*",
        ]
    )


def test_no_timeline_without_the_flag(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_demo=_FLAKY)
    result = pytester.runpytest_subprocess()
    result.assert_outcomes(failed=1)
    result.stdout.no_fnmatch_line("*timeline:*")
    assert not list(pytester.path.glob("simloop-timeline-*.html"))


def test_no_timeline_for_a_passing_test(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


@sim_test(seeds=3)
async def test_clean():
    await asyncio.sleep(0.1)
"""
    )
    result = pytester.runpytest_subprocess("--simloop-timeline")
    result.assert_outcomes(passed=1)
    assert not list(pytester.path.glob("simloop-timeline-*.html"))


def test_timeline_covers_the_whole_run_not_just_the_report_tail(
    pytester: pytest.Pytester,
) -> None:
    # The failure report prints a tail of the trace; the page is the run.
    pytester.makepyfile(
        test_demo="""
import asyncio
from simloop import sim_test


def tick():
    pass


@sim_test(seeds=10, trace_tail=5)
async def test_flaky():
    loop = asyncio.get_running_loop()
    for _ in range(50):
        loop.call_soon(tick)
        await asyncio.sleep(0)
    assert loop.seed != 3
"""
    )
    result = pytester.runpytest_subprocess("--simloop-timeline")
    result.assert_outcomes(failed=1)
    page = (pytester.path / "simloop-timeline-seed3.html").read_text(encoding="utf-8")
    assert page.count('class="dot"') > 50


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
