"""Cluster assembly and fault choreography shared across the sim suites."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from simloop import SimLoop

from jobqueue.broker import Broker
from jobqueue.client import Client
from jobqueue.store import EffectStore
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
