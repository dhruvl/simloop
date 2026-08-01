"""Leases held by a worker whose clock runs two leases fast.

The broker is the only clock that decides expiry, and everything the
protocol puts on the wire is a duration, never a timestamp. These runs
put a badly skewed worker in the middle of the lease machinery and check
that the exactly-once story is unchanged.
"""

from __future__ import annotations

import asyncio

import pytest
from simloop import SimLoop, sim_test

import helpers

LEASE_S = 2.0  # the broker's default lease
SKEW_S = 2 * LEASE_S  # a clock two full leases ahead of the rest of the cluster


async def _clock_reading() -> float:
    return helpers.sim_loop().time()


@sim_test(seeds=25)
async def test_a_fast_clock_does_not_cost_the_worker_its_lease() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=1, clients=1)
    loop.net.set_clock("w1", offset=SKEW_S)
    # The skew is real from inside the worker: its tasks read a clock two
    # leases ahead of the broker's.
    worker_now = await loop.net.host("w1").create_task(_clock_reading())
    assert worker_now - loop.time() == pytest.approx(SKEW_S)
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("fast-clock", duration=1.5 * LEASE_S)
    )
    assert job_id is not None
    await helpers.settle(cluster)
    helpers.verify(cluster)
    # Renewals are heartbeats on a timer, not deadline arithmetic, so the
    # skewed worker keeps the lease it was granted: one attempt, one commit,
    # no re-run to fence.
    assert cluster.broker.snapshot()[job_id] == ("done", 1)
    assert [(c.token, c.value) for c in cluster.store.commits] == [(1, "fast-clock")]
    assert cluster.store.rejected == []


@sim_test(seeds=50)
async def test_a_fast_worker_takes_over_a_lapsed_lease() -> None:
    loop = helpers.sim_loop()
    loop.net.host("w2")  # register it early so it can be skewed before it runs
    loop.net.set_clock("w2", offset=SKEW_S)
    # w1 is partitioned mid-job and turns into a zombie; the skewed w2 is the
    # worker that picks the job back up once the broker expires the lease.
    cluster = await helpers.zombie_run(helpers.EffectStore())
    helpers.verify(cluster)
    assert any(reason == "stale" for _, _, reason in cluster.store.rejected)
    assert len(cluster.store.commits) == 1
    assert cluster.store.commits[0].token >= 2  # the re-run's lease, not the zombie's


@sim_test(seeds=25)
async def test_skewed_and_honest_workers_share_a_queue() -> None:
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=1)
    loop.net.set_clock("w2", offset=SKEW_S)
    client = cluster.clients[0]
    host = loop.net.host("c1")
    job_ids = [await host.create_task(client.submit(f"m{i}")) for i in range(4)]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster)
    helpers.verify(cluster)
    assert len(cluster.store.commits) == 4


async def _skew_workload() -> str:
    """Run a small cluster to quiesce; return the trace hash of that run."""
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(workers=2, clients=1)
    client = cluster.clients[0]
    host = loop.net.host("c1")
    job_ids = [await host.create_task(client.submit(f"t{i}")) for i in range(2)]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster)
    helpers.verify(cluster)
    trace = loop.trace_hash()  # taken at quiesce: the run, not the teardown
    # Then shut everything down — servers, workers, open connection handlers —
    # so the loop closes with nothing suspended mid-write. `all_tasks()` is a
    # set, so sort it by name: the teardown cannot smuggle in id ordering.
    # (The hash above is taken before any of this runs, deliberately.)
    here = asyncio.current_task()
    running = sorted(
        (task for task in asyncio.all_tasks() if task is not here),
        key=lambda task: task.get_name(),
    )
    for task in running:
        task.cancel()
    await asyncio.gather(*running, return_exceptions=True)
    await asyncio.sleep(0.1)  # let cancelled renew heartbeats finish unwinding
    return trace


def _skew_hash(seed: int, offset: float) -> str:
    loop = SimLoop(seed=seed)
    # Every run registers the hosts in the same order; only the offset differs.
    for name in ("broker", "w1", "w2", "c1"):
        loop.net.host(name)
    loop.net.set_clock("w2", offset=offset)
    try:
        trace: str = loop.run_until_complete(_skew_workload())
    finally:
        loop.close()
    return trace


def test_skew_alone_does_not_change_what_happens() -> None:
    # Why the skew is absorbed rather than merely survived: no timestamp ever
    # crosses a host boundary here. The broker sends `lease_s`, a duration;
    # the worker sleeps and heartbeats on durations; only the broker compares
    # readings, all of them its own. A worker's wrong clock is therefore
    # unobservable — the packet trace is byte-identical either way, which is
    # also why no ablation of *this* app can turn skew into a violation.
    for seed in (0, 7):
        hashes = {_skew_hash(seed, offset) for offset in (0.0, SKEW_S, -SKEW_S)}
        assert len(hashes) == 1, f"seed {seed}: the skew moved the trace"
