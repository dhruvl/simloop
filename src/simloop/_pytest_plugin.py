"""pytest integration: seed-count and replay options, replay lines.

pytest loads this module through the ``pytest11`` entry point declared in
pyproject.toml. simloop itself never imports it, so the library keeps its
zero-dependency import surface.
"""

from __future__ import annotations

import fnmatch
import os

import pytest

from simloop import _explore
from simloop._explore import POLICIES
from simloop._shrink import DEFAULT_BUDGET


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("simloop")
    group.addoption(
        "--simloop-seeds",
        type=int,
        default=None,
        metavar="N",
        help="run every @sim_test under seeds 0..N-1, overriding decorators",
    )
    group.addoption(
        "--simloop-replay",
        type=int,
        default=None,
        metavar="SEED",
        help="run every @sim_test at exactly this seed",
    )
    group.addoption(
        "--simloop-shrink",
        action="store_true",
        default=False,
        help="minimize the failing seed's schedule (experimental, costs runs)",
    )
    group.addoption(
        "--simloop-shrink-budget",
        type=int,
        default=DEFAULT_BUDGET,
        metavar="N",
        help="runs the schedule shrinker may spend on one failure",
    )
    group.addoption(
        "--simloop-jobs",
        type=int,
        default=1,
        metavar="N",
        help="explore each @sim_test's seeds across N worker processes",
    )
    group.addoption(
        "--simloop-policy",
        choices=POLICIES,
        default=None,
        help="how the scheduler picks: seeded uniform draws, or PCT priorities",
    )
    group.addoption(
        "--simloop-pct-depth",
        type=int,
        default=None,
        metavar="N",
        help="ordering constraints PCT aims to hit (--simloop-policy=pct only)",
    )
    group.addoption(
        "--simloop-timeline",
        nargs="?",
        const="",
        default=None,
        metavar="DIR",
        help="draw each failing seed's trace to an HTML page, in DIR if given "
        "(as --simloop-timeline=DIR) or where pytest was invoked",
    )


def pytest_configure(config: pytest.Config) -> None:
    _explore.overrides.seeds = config.getoption("--simloop-seeds")
    _explore.overrides.replay = config.getoption("--simloop-replay")
    _explore.overrides.shrink = config.getoption("--simloop-shrink")
    budget = config.getoption("--simloop-shrink-budget")
    if budget < 1:
        # Caught here rather than at the first failure, which could be many
        # minutes into a session.
        raise pytest.UsageError("--simloop-shrink-budget must be at least 1")
    _explore.overrides.shrink_budget = budget
    jobs = config.getoption("--simloop-jobs")
    if jobs < 1:
        raise pytest.UsageError("--simloop-jobs must be at least 1")
    _explore.overrides.jobs = jobs
    policy = config.getoption("--simloop-policy")
    depth = config.getoption("--simloop-pct-depth")
    if depth is not None and depth < 1:
        raise pytest.UsageError("--simloop-pct-depth must be at least 1")
    if policy == "pct" and jobs > 1:
        # The horizon PCT works to is measured in this process and does not
        # reach a worker, so the combination is refused here rather than at
        # the first sim test that would have explored under it.
        raise pytest.UsageError(
            "--simloop-policy=pct explores sequentially: drop --simloop-jobs"
        )
    _explore.overrides.policy = policy
    _explore.overrides.pct_depth = depth
    _explore.overrides.timeline_dir = _timeline_dir(config)
    _explore.overrides.sim_tests = 0
    _explore.overrides.seeds_explored = 0


def _is_test_path(value: str, config: pytest.Config) -> bool:
    """Would pytest have collected this argument, had we not eaten it?

    A node id or an existing file is one. A directory is one only if there is
    something to collect under it — an empty ``artifacts/`` a user made for
    the pages themselves is a directory and nothing more.
    """
    if "::" in value:
        return True
    if os.path.isfile(value):
        return True
    if not os.path.isdir(value):
        return False
    patterns = config.getini("python_files")
    for _, _, files in os.walk(value):
        if any(fnmatch.fnmatch(name, pattern) for name in files for pattern in patterns):
            return True
    return False


def _timeline_dir(config: pytest.Config) -> str | None:
    """Where failing seeds' timelines go, or ``None`` when nobody asked.

    The directory is optional, which means argparse would otherwise read the
    next argument as one: ``pytest --simloop-timeline tests/`` would collect
    nothing it was told to, run the whole suite instead, and drop the pages
    into the test tree. An argument pytest would have collected is refused.
    By the time the value reaches here the two spellings are the same string,
    so ``--simloop-timeline=tests`` is refused as well; the message names the
    ways out rather than a syntax the user may already be using.
    """
    directory: str | None = config.getoption("--simloop-timeline")
    if directory is None:
        return None
    if directory and _is_test_path(directory, config):
        raise pytest.UsageError(
            f"--simloop-timeline wants a directory, and {directory!r} is a "
            "test path pytest would have collected: pass "
            "--simloop-timeline with no value to write the pages where "
            "pytest was invoked, or name a directory that is not also a "
            "test path"
        )
    # Written where the run was started from, which is where pytest's own
    # artifacts land and what a relative path in the report is relative to.
    return directory or str(config.invocation_params.dir)


def pytest_unconfigure(config: pytest.Config) -> None:
    _explore.overrides.seeds = None
    _explore.overrides.replay = None
    _explore.overrides.shrink = False
    _explore.overrides.shrink_budget = DEFAULT_BUDGET
    _explore.overrides.jobs = 1
    _explore.overrides.policy = None
    _explore.overrides.pct_depth = None
    _explore.overrides.timeline_dir = None
    _explore.overrides.node_id = None


def pytest_runtest_setup(item: pytest.Item) -> None:
    _explore.overrides.node_id = item.nodeid


def pytest_runtest_teardown(item: pytest.Item) -> None:
    _explore.overrides.node_id = None


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    tests = _explore.overrides.sim_tests
    if not tests:
        return
    seeds = _explore.overrides.seeds_explored
    noun = "sim test" if tests == 1 else "sim tests"
    terminalreporter.write_line(
        f"simloop: {tests} {noun}, {seeds:,} seeds explored"
    )
