"""Hundreds of seeds of chaos; the four safety claims must hold on every one."""

from __future__ import annotations

import asyncio

import pytest

from simloop import sim, sim_test

import harness


@pytest.mark.slow
@sim_test(seeds=300)
async def test_chaos_campaign_holds_the_invariants() -> None:
    rng = sim.random
    loop = harness.sim_loop()
    cluster = await harness.start_cluster(size=5)
    loop.net.set_defaults(latency=(0.01, 0.05), drop=0.02, duplicate=0.02)
    for i in range(2):
        await harness.propose(cluster, f"before.{i}")
    disorder = loop.create_task(harness.chaos(cluster, rng))
    sent = 0
    while not disorder.done():
        await harness.propose(cluster, f"during.{sent}", timeout_s=120.0)
        sent += 1
        await asyncio.sleep(rng.uniform(0.3, 1.0))
    await disorder
    loop.net.heal()
    await harness.propose(cluster, "after", timeout_s=120.0)
    await harness.settle(cluster)
    harness.verify(cluster)
