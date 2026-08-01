"""pytest integration: seed-count and replay options, replay lines.

pytest loads this module through the ``pytest11`` entry point declared in
pyproject.toml. simloop itself never imports it, so the library keeps its
zero-dependency import surface.
"""

from __future__ import annotations

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
    _explore.overrides.sim_tests = 0
    _explore.overrides.seeds_explored = 0


def pytest_unconfigure(config: pytest.Config) -> None:
    _explore.overrides.seeds = None
    _explore.overrides.replay = None
    _explore.overrides.shrink = False
    _explore.overrides.shrink_budget = DEFAULT_BUDGET
    _explore.overrides.jobs = 1
    _explore.overrides.policy = None
    _explore.overrides.pct_depth = None
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
