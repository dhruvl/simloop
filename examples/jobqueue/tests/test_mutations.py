"""Ablations: switch one safeguard off and prove the explorer catches it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from simloop import SeedReport, explore

import helpers
from invariants import InvariantViolation

BUDGET = 200


def _find(scenario: Callable[[], Coroutine[Any, Any, object]]) -> SeedReport:
    report = explore(scenario, range(BUDGET))
    assert report is not None, "ablation went undetected across the seed budget"
    return report


def test_unfenced_store_admits_a_zombie_write() -> None:
    async def scenario() -> None:
        cluster = await helpers.zombie_run(helpers.EffectStore(fenced=False))
        helpers.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant == "no-zombie-writes"


def test_unidempotent_store_double_commits_after_a_crash() -> None:
    async def scenario() -> None:
        store = helpers.CrashOnFirstCommit("w1", idempotent=False)
        cluster = await helpers.crash_after_commit_run(store)
        helpers.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant == "exactly-once"


def test_broker_fencing_off_lets_a_zombie_finish_the_job() -> None:
    # Broker fencing is second-line defense: with the store intact nothing
    # stale ever reaches the broker, so its ablation is shown paired with a
    # fully ablated store — the zombie both writes and completes.
    async def scenario() -> None:
        cluster = await helpers.zombie_run(
            helpers.EffectStore(fenced=False, idempotent=False),
            broker=helpers.Broker(fencing=False),
        )
        helpers.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant in ("exactly-once", "no-zombie-writes")


def test_no_idempotency_key_duplicates_a_lost_ack_submit() -> None:
    async def scenario() -> None:
        loop = helpers.sim_loop()
        cluster = await helpers.start_cluster(
            workers=1, clients=1, client_kwargs={"idempotency": False}
        )
        submit_task = loop.net.host("c1").create_task(
            cluster.clients[0].submit("d", duration=0.1)
        )
        await asyncio.sleep(0.12)
        loop.net.partition(["c1"], ["broker"])
        await asyncio.sleep(1.5)
        loop.net.heal()
        job_id = await submit_task
        assert job_id is not None
        await helpers.settle(cluster)
        helpers.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant == "exactly-once"


def test_unbounded_attempts_never_converge_on_poison() -> None:
    async def scenario() -> None:
        loop = helpers.sim_loop()
        broker = helpers.Broker(max_attempts=None, lease_s=0.5, backoff_base_s=0.1)
        cluster = await helpers.start_cluster(workers=1, clients=1, broker=broker)
        job_id = await loop.net.host("c1").create_task(
            cluster.clients[0].submit("p", duration=0.05, poison=True)
        )
        assert job_id is not None
        await helpers.settle(cluster, timeout_s=30.0)

    report = _find(scenario)
    assert report.seed == 0  # every seed spins forever; the first one shows it
    assert isinstance(report.exception, TimeoutError)


def test_renew_off_paired_with_unidempotent_store_double_commits() -> None:
    async def scenario() -> None:
        store = helpers.EffectStore(idempotent=False)
        cluster = await helpers.slow_job_run(store)
        helpers.verify(cluster)

    report = _find(scenario)
    assert isinstance(report.exception, InvariantViolation)
    assert report.exception.invariant == "exactly-once"


@pytest.mark.slow
def test_renew_off_alone_stays_safe() -> None:
    # Defense in depth: without renewals jobs re-run more, but the intact
    # store still commits each effect exactly once across every seed.
    async def scenario() -> None:
        cluster = await helpers.slow_job_run(helpers.EffectStore())
        helpers.verify(cluster)

    assert explore(scenario, range(75)) is None


@pytest.mark.slow
def test_broker_fencing_off_alone_stays_safe() -> None:
    # Defense in depth: broker-side fencing is second-line. With the effect
    # store intact, a zombie's stale commit is refused at the store no matter
    # what the broker does, so turning broker fencing off alone stays safe
    # across every seed.
    async def scenario() -> None:
        cluster = await helpers.zombie_run(
            helpers.EffectStore(),
            broker=helpers.Broker(fencing=False),
        )
        helpers.verify(cluster)

    assert explore(scenario, range(75)) is None
