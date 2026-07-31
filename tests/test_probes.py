"""The probe harness: its verdict vocabulary and the contract probes keep.

These tests never install a third-party library, so they exercise the harness
rather than the compatibility table it produces: probe modules must stay
importable — and their metadata readable — with the ``probes`` dependency
group absent, which is what lets `uv run pytest` stay dependency-free.
"""

from __future__ import annotations

import asyncio
import inspect
from types import ModuleType

import pytest

from probes import _runner, report
from simloop import SimLoop, SimulationFenceError

PROBE_MODULES = report.modules()


async def _returns(loop: SimLoop) -> str:
    await asyncio.sleep(1.0)
    return f"nothing at all, until {loop.time()}s"


async def _fences(loop: SimLoop) -> str:
    loop.call_soon_threadsafe(print)
    return "unreachable"


async def _fences_inside_a_group(loop: SimLoop) -> str:
    async with asyncio.TaskGroup() as group:
        group.create_task(_fences(loop))
    return "unreachable"


async def _fences_behind_a_cause(loop: SimLoop) -> str:
    try:
        loop.call_soon_threadsafe(print)
    except SimulationFenceError as fence:
        raise RuntimeError("the library wrapped it") from fence
    return "unreachable"


async def _raises(loop: SimLoop) -> str:
    raise ValueError("no such endpoint")


async def _raises_without_a_message(loop: SimLoop) -> str:
    raise ConnectionResetError


async def _never_finishes(loop: SimLoop) -> str:
    await asyncio.sleep(10_000.0)
    return "unreachable"


def test_a_probe_that_returns_reports_what_it_exercised() -> None:
    assert _runner.run(_returns) == "works: nothing at all, until 1.0s"


def test_a_fence_is_reported_verbatim() -> None:
    verdict = _runner.run(_fences)
    assert verdict == (
        "fenced: simloop does not simulate 'call_soon_threadsafe'; "
        "see docs/supported-api.md for the supported asyncio subset"
    )


@pytest.mark.parametrize("probe", [_fences_inside_a_group, _fences_behind_a_cause])
def test_a_wrapped_fence_is_still_a_fence(probe: _runner.Probe) -> None:
    # Libraries rarely let the fence out untouched: task groups bundle it and
    # clients re-raise their own error from it.
    assert _runner.run(probe).startswith("fenced: simloop does not simulate")


def test_a_failure_is_reported_as_itself() -> None:
    assert _runner.run(_raises) == "fails: ValueError: no such endpoint"


def test_a_failure_without_a_message_is_named_by_type() -> None:
    assert _runner.run(_raises_without_a_message) == "fails: ConnectionResetError"


def test_the_budget_bounds_a_probe_that_never_finishes() -> None:
    assert _runner.run(_never_finishes, budget=5.0) == (
        "fails: BudgetExceeded: still running after 5.0s"
    )


def test_probes_are_discovered_in_a_stable_order() -> None:
    names = [module.__name__ for module in report.modules()]
    assert names == sorted(names)
    assert "probes.probe_anyio" in names


@pytest.mark.parametrize("module", PROBE_MODULES, ids=lambda m: m.__name__)
def test_every_probe_module_keeps_the_harness_contract(module: ModuleType) -> None:
    assert isinstance(module.LIBRARY, str) and module.LIBRARY
    assert module.DISTRIBUTION is None or isinstance(module.DISTRIBUTION, str)
    assert module.TIER in (1, 2)
    assert isinstance(module.NOTES, str) and module.NOTES
    assert inspect.iscoroutinefunction(module.probe)
    assert list(inspect.signature(module.probe).parameters) == ["loop"]


def test_a_missing_distribution_is_named_rather_than_guessed() -> None:
    assert report._version("simloop") != "not installed"
    assert report._version("no-such-distribution") == "not installed"
    assert report._version(None) == "n/a"


def test_the_table_is_markdown_with_one_row_per_probe() -> None:
    rows = [
        report.Row(tier=2, library="b", version="2.0", verdict="fenced: x", notes="n"),
        report.Row(tier=1, library="a", version="1.0", verdict="works: y | z", notes=""),
    ]
    lines = report.table(sorted(rows, key=lambda row: (row.tier, row.library))).split(
        "\n"
    )
    assert lines[0] == "| Library | Version | Verdict | Notes |"
    assert lines[1] == "|---|---|---|---|"
    # Tier 1 first, and a pipe inside a verdict must not split the row.
    assert lines[2] == r"| a | 1.0 | works: y \| z |  |"
    assert lines[3] == "| b | 2.0 | fenced: x | n |"
