"""Whole-cluster scenarios under seed exploration."""

from __future__ import annotations

import asyncio

import pytest
from simloop import SimLoop, sim, sim_test

import helpers


@sim_test(seeds=25)
async def test_one_job_end_to_end() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=1, clients=1)
    client = cluster.clients[0]
    job_id = await loop.net.host("c1").create_task(client.submit("v1"))
    assert job_id is not None
    state = await loop.net.host("c1").create_task(client.wait(job_id))
    assert state == "done"
    assert [commit.value for commit in cluster.store.commits] == ["v1"]
    assert cluster.broker.snapshot()[job_id] == ("done", 1)


@sim_test(seeds=25)
async def test_happy_path_many_jobs_commit_once() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=2)
    submits = []
    for i, client in enumerate(cluster.clients):
        host = loop.net.host(f"c{i + 1}")
        for j in range(3):
            submits.append(host.create_task(client.submit(f"v{i}.{j}")))
    job_ids = [await task for task in submits]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster)
    helpers.verify(cluster)
    assert len(cluster.store.commits) == 6


@sim_test(seeds=50)
async def test_worker_crash_mid_job_still_commits_once() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=1)
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("crashy", duration=1.0)
    )
    assert job_id is not None
    await asyncio.sleep(0.2 + sim.random.uniform(0.0, 1.2))
    loop.net.host("w1").crash()
    helpers.add_worker(cluster, "w3")
    await helpers.settle(cluster)
    helpers.verify(cluster)
    assert cluster.broker.snapshot()[job_id][0] == "done"


@sim_test(seeds=50)
async def test_partitioned_zombie_is_fenced() -> None:
    cluster = await helpers.zombie_run(helpers.EffectStore())
    helpers.verify(cluster)
    # the zombie's late commit was refused; the re-run's commit won
    assert any(reason == "stale" for _, _, reason in cluster.store.rejected)
    assert len(cluster.store.commits) == 1
    assert cluster.store.commits[0].token >= 2


@sim_test(seeds=50)
async def test_lost_submit_ack_does_not_duplicate_the_job() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=1, clients=1)
    submit_task = loop.net.host("c1").create_task(
        cluster.clients[0].submit("d", duration=0.1)
    )
    await asyncio.sleep(0.12)  # the request (or its ack) is now in flight
    loop.net.partition(["c1"], ["broker"])
    await asyncio.sleep(1.5)  # the client times out and starts retrying
    loop.net.heal()
    job_id = await submit_task
    assert job_id is not None
    await helpers.settle(cluster)
    helpers.verify(cluster)
    assert len(cluster.broker.snapshot()) == 1  # dedupe: one job, however many tries


@sim_test(seeds=50)
async def test_crash_between_commit_and_complete_is_absorbed() -> None:
    store = helpers.CrashOnFirstCommit("w1")
    cluster = await helpers.crash_after_commit_run(store)
    helpers.verify(cluster)
    assert len(cluster.store.commits) == 1
    assert any(reason == "duplicate" for _, _, reason in cluster.store.rejected)


@sim_test(seeds=25)
async def test_poison_job_dead_letters_without_collateral() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=1)
    client = cluster.clients[0]
    good = await loop.net.host("c1").create_task(client.submit("good"))
    bad = await loop.net.host("c1").create_task(client.submit("bad", poison=True))
    assert good is not None and bad is not None
    await helpers.settle(cluster)
    helpers.verify(cluster)
    assert cluster.broker.snapshot()[good] == ("done", 1)
    assert cluster.broker.snapshot()[bad] == ("dead", 3)
    assert [commit.value for commit in cluster.store.commits] == ["good"]


async def _replay_workload() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=1)
    client = cluster.clients[0]
    host = loop.net.host("c1")
    job_ids = [await host.create_task(client.submit(f"r{i}")) for i in range(3)]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster)
    helpers.verify(cluster)
    for task in cluster.tasks:
        task.cancel()
    await asyncio.gather(*cluster.tasks, return_exceptions=True)
    await asyncio.sleep(0.1)  # let cancelled renew heartbeats finish unwinding


def _replay_hash(seed: int) -> str:
    loop = SimLoop(seed=seed)
    try:
        loop.run_until_complete(_replay_workload())
    finally:
        loop.close()
    return loop.trace_hash()


@pytest.mark.slow
def test_cluster_replay_is_stable() -> None:
    for seed in (0, 7):
        hashes = {_replay_hash(seed) for _ in range(5)}
        assert len(hashes) == 1, f"seed {seed} produced diverging traces"
