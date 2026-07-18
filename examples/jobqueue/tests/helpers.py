"""Cluster assembly and fault choreography shared across the sim suites."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from simloop import SimLoop

from invariants import check_invariants
from jobqueue.broker import Broker as Broker
from jobqueue.client import Client
from jobqueue.store import EffectStore as EffectStore
from jobqueue.worker import Worker


def sim_loop() -> SimLoop:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    return loop


@dataclass
class Cluster:
    broker: Broker
    store: EffectStore
    clients: list[Client]
    worker_hosts: list[str]
    tasks: list[asyncio.Task[Any]]


def add_worker(cluster: Cluster, host_name: str, **kwargs: Any) -> None:
    loop = sim_loop()
    worker = Worker(host_name, cluster.store, **kwargs)
    task = loop.net.host(host_name).create_task(worker.run(), name=host_name)
    cluster.worker_hosts.append(host_name)
    cluster.tasks.append(task)


async def start_cluster(
    *,
    workers: int = 2,
    clients: int = 1,
    store: EffectStore | None = None,
    broker: Broker | None = None,
    worker_kwargs: dict[str, Any] | None = None,
    client_kwargs: dict[str, Any] | None = None,
) -> Cluster:
    loop = sim_loop()
    loop.net.set_defaults(latency=(0.01, 0.05))
    cluster = Cluster(
        broker=broker if broker is not None else Broker(),
        store=store if store is not None else EffectStore(),
        clients=[],
        worker_hosts=[],
        tasks=[],
    )
    cluster.tasks.append(
        loop.net.host("broker").create_task(cluster.broker.serve(), name="broker")
    )
    for i in range(workers):
        add_worker(cluster, f"w{i + 1}", **(worker_kwargs or {}))
    for i in range(clients):
        name = f"c{i + 1}"
        loop.net.host(name)
        cluster.clients.append(Client(name, **(client_kwargs or {})))
    await asyncio.sleep(0.05)  # let the broker start listening
    return cluster


def acknowledged(cluster: Cluster) -> dict[str, str]:
    acked: dict[str, str] = {}
    for client in cluster.clients:
        acked.update(client.acknowledged)
    return acked


async def settle(cluster: Cluster, *, timeout_s: float = 120.0) -> None:
    """Wait (in virtual time) until every acknowledged job is done or dead."""
    async with asyncio.timeout(timeout_s):
        while True:
            jobs = cluster.broker.snapshot()
            acked = acknowledged(cluster).values()
            if all(jobs[job_id][0] in ("done", "dead") for job_id in acked):
                return
            await asyncio.sleep(0.25)


def verify(cluster: Cluster) -> None:
    check_invariants(cluster.store, cluster.broker, acknowledged(cluster))


class CrashOnFirstCommit(EffectStore):
    """Crashes ``host`` the instant its first commit is accepted.

    Models a worker dying between applying its effect and telling the
    broker: the job re-runs, and only the store's idempotent apply keeps
    the effect single.
    """

    def __init__(self, host: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._crash_host = host
        self._fired = False

    def commit(self, job_id: str, token: int, value: str) -> str:
        result = super().commit(job_id, token, value)
        if not self._fired and result == "ok":
            self._fired = True
            sim_loop().net.host(self._crash_host).crash()
        return result


async def zombie_run(store: EffectStore, *, broker: Broker | None = None) -> Cluster:
    """One slow job; its first worker is partitioned into a zombie mid-run."""
    loop = sim_loop()
    cluster = await start_cluster(workers=1, clients=1, store=store, broker=broker)
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("z", duration=3.0)
    )
    assert job_id is not None
    # Wait for w1 to actually start executing (not merely for the broker to
    # grant the lease — the grant response could still be in flight, and
    # partitioning then would strand it, leaving no zombie at all).
    async with asyncio.timeout(5.0):
        while not store.begun:
            await asyncio.sleep(0.05)
    loop.net.partition(["w1"], ["broker"])  # renewals now go nowhere
    add_worker(cluster, "w2")
    await asyncio.sleep(4.0)  # the lease lapses; w2 re-runs while w1 is a zombie
    loop.net.heal()
    await settle(cluster, timeout_s=60.0)
    return cluster


async def crash_after_commit_run(store: EffectStore) -> Cluster:
    """The first worker dies right after committing, before completing."""
    loop = sim_loop()
    cluster = await start_cluster(workers=1, clients=1, store=store)
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("c", duration=0.2)
    )
    assert job_id is not None
    async with asyncio.timeout(10.0):
        while not store.commits:
            await asyncio.sleep(0.05)
    add_worker(cluster, "w2")  # replacement capacity; w1 is gone
    await settle(cluster, timeout_s=60.0)
    return cluster


async def slow_job_run(store: EffectStore) -> Cluster:
    """No renewals and a job slower than its lease: it must re-run."""
    loop = sim_loop()
    broker = Broker(lease_s=1.0, backoff_base_s=0.25)
    cluster = await start_cluster(
        workers=1,
        clients=1,
        store=store,
        broker=broker,
        worker_kwargs={"renew": False},
    )
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("s", duration=1.5)
    )
    assert job_id is not None
    await settle(cluster, timeout_s=60.0)
    return cluster
