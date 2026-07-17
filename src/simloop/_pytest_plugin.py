"""pytest integration: seed-count and replay options, replay lines.

pytest loads this module through the ``pytest11`` entry point declared in
pyproject.toml. simloop itself never imports it, so the library keeps its
zero-dependency import surface.
"""

from __future__ import annotations

import pytest

from simloop import _explore


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


def pytest_configure(config: pytest.Config) -> None:
    _explore.overrides.seeds = config.getoption("--simloop-seeds")
    _explore.overrides.replay = config.getoption("--simloop-replay")
    _explore.overrides.sim_tests = 0
    _explore.overrides.seeds_explored = 0


def pytest_unconfigure(config: pytest.Config) -> None:
    _explore.overrides.seeds = None
    _explore.overrides.replay = None
    _explore.overrides.node_id = None


def pytest_runtest_setup(item: pytest.Item) -> None:
    _explore.overrides.node_id = item.nodeid


def pytest_runtest_teardown(item: pytest.Item) -> None:
    _explore.overrides.node_id = None
