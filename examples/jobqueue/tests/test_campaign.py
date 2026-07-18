"""300 seeds of chaos: random faults, every invariant must hold."""

from __future__ import annotations

import pytest

from simloop import sim, sim_test

import helpers


@pytest.mark.slow
@sim_test(seeds=300)
async def test_campaign_holds_invariants_under_chaos() -> None:
    loop = helpers.sim_loop()
    rng = sim.random
    cluster = await helpers.start_cluster(workers=3, clients=2)
    submits = []
    for i, client in enumerate(cluster.clients):
        host = loop.net.host(f"c{i + 1}")
        for j in range(4):
            submits.append(
                host.create_task(
                    client.submit(
                        f"c{i + 1}.{j}",
                        duration=rng.uniform(0.05, 1.0),
                        poison=rng.random() < 0.15,
                    )
                )
            )
    chaos_task = loop.create_task(helpers.chaos(cluster, rng))
    job_ids = [await task for task in submits]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster, timeout_s=600.0)
    chaos_task.cancel()
    helpers.verify(cluster)
