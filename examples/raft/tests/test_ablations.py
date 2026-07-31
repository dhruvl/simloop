"""Ablations: switch one safeguard off and prove the explorer catches it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from simloop import SeedReport, explore

from raft.node import Safeguards

import harness
from checks import InvariantViolation

BUDGET = 300

Scenario = Callable[[], Coroutine[Any, Any, None]]


def _find(scenario: Scenario, budget: int = BUDGET) -> SeedReport:
    report = explore(scenario, range(budget))
    assert report is not None, "ablation went undetected across the seed budget"
    return report


def test_double_voting_elects_two_leaders_in_one_term() -> None:
    async def scenario() -> None:
        cluster = await harness.start_cluster(
            safeguards=Safeguards(one_vote_per_term=False)
        )
        loop = harness.sim_loop()
        leader = await harness.wait_for_leader(cluster)
        rest = [name for name in cluster.names if name != leader]
        for _ in range(2):
            loop.net.partition([leader], rest)
            await asyncio.sleep(1.5)
            loop.net.heal()
            await asyncio.sleep(0.5)
            harness.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant == "election-safety"


def test_unchecked_logs_let_a_stale_follower_lead() -> None:
    async def scenario() -> None:
        cluster = await harness.start_cluster(
            safeguards=Safeguards(check_log_up_to_date=False)
        )
        loop = harness.sim_loop()
        leader = await harness.wait_for_leader(cluster)
        rest = [name for name in cluster.names if name != leader]
        behind = rest[0]
        loop.net.partition([behind], [leader, rest[1]])
        for i in range(3):
            await harness.propose(cluster, f"k{i}")
        loop.net.heal()
        loop.net.partition([leader], [behind, rest[1]])
        await asyncio.sleep(2.0)
        harness.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant in (
        "leader-completeness", "state-machine-safety",
    )


def test_skipped_persistence_forgets_committed_entries() -> None:
    async def scenario() -> None:
        cluster = await harness.start_cluster(
            safeguards=Safeguards(persist_before_reply=False)
        )
        await harness.wait_for_leader(cluster)
        for i in range(2):
            await harness.propose(cluster, f"k{i}")
        for name in list(cluster.names):
            await harness.restart(cluster, name)
            await asyncio.sleep(0.5)
        await asyncio.sleep(2.0)
        harness.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant in (
        "leader-completeness", "election-safety", "state-machine-safety",
    )


def test_accepting_stale_terms_rewrites_history() -> None:
    async def scenario() -> None:
        cluster = await harness.start_cluster(
            safeguards=Safeguards(reject_stale_term=False)
        )
        loop = harness.sim_loop()
        first = await harness.wait_for_leader(cluster)
        await harness.propose(cluster, "k0")
        rest = [name for name in cluster.names if name != first]
        loop.net.partition([first], rest)
        second = await harness.wait_for_leader(cluster)
        await harness.propose(cluster, "k1")
        await harness.propose(cluster, "k2")
        loop.net.heal()
        loop.net.partition([second], [name for name in cluster.names if name != second])
        await asyncio.sleep(3.0)
        loop.net.heal()
        await harness.propose(cluster, "k3", timeout_s=60.0)
        await asyncio.sleep(1.0)
        harness.verify(cluster)

    report = _find(scenario)
    # No invariant pinned, deliberately: a node that answers superseded terms
    # takes both stale appends and stale vote grants, which puts all four
    # claims genuinely in reach -- naming all four would say nothing.
    assert isinstance(report.exception, InvariantViolation)


def test_committing_old_terms_by_count_loses_writes() -> None:
    async def scenario() -> None:
        cluster = await harness.figure_eight(
            Safeguards(commit_own_term_only=False, leader_noop=False)
        )
        harness.verify(cluster)

    report = _find(scenario, budget=500)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant in (
        "state-machine-safety", "leader-completeness",
    )


@pytest.mark.slow
def test_the_commit_gate_alone_keeps_history() -> None:
    async def scenario() -> None:
        cluster = await harness.figure_eight(Safeguards(leader_noop=False))
        harness.verify(cluster)

    assert explore(scenario, range(150)) is None


@pytest.mark.slow
def test_the_vote_ledger_alone_keeps_elections_single() -> None:
    async def scenario() -> None:
        cluster = await harness.start_cluster()
        loop = harness.sim_loop()
        leader = await harness.wait_for_leader(cluster)
        rest = [name for name in cluster.names if name != leader]
        for _ in range(2):
            loop.net.partition([leader], rest)
            await asyncio.sleep(1.5)
            loop.net.heal()
            await asyncio.sleep(0.5)
            harness.verify(cluster)

    assert explore(scenario, range(150)) is None
