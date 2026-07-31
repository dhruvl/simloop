"""Cluster assembly and fault choreography shared across the raft suites."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from simloop import SimLoop, sim

from raft.node import LEADER, Event, RaftNode, Safeguards
from raft.storage import Entry, MemoryStorage


def sim_loop() -> SimLoop:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    return loop


@dataclass
class Member:
    name: str
    storage: MemoryStorage
    node: RaftNode
    task: asyncio.Task[Any]


@dataclass
class Cluster:
    names: list[str]
    members: dict[str, Member]
    events: list[Event]
    safeguards: Safeguards

    def logs(self) -> dict[str, tuple[Entry, ...]]:
        return {name: member.node.log for name, member in self.members.items()}


async def start_cluster(
    *, size: int = 3, safeguards: Safeguards | None = None
) -> Cluster:
    loop = sim_loop()
    loop.net.set_defaults(latency=(0.01, 0.05))
    cluster = Cluster(
        names=[f"n{i + 1}" for i in range(size)],
        members={},
        events=[],
        safeguards=safeguards if safeguards is not None else Safeguards(),
    )
    for name in cluster.names:
        _boot(cluster, name, MemoryStorage())
    await asyncio.sleep(0.05)  # let the servers start listening
    return cluster


def _boot(cluster: Cluster, name: str, storage: MemoryStorage) -> None:
    loop = sim_loop()
    node = RaftNode(
        name,
        [peer for peer in cluster.names if peer != name],
        storage,
        rng=sim.random,
        safeguards=cluster.safeguards,
        events=cluster.events,
    )
    task = loop.net.host(name).create_task(node.run(), name=name)
    cluster.members[name] = Member(name, storage, node, task)


async def restart(cluster: Cluster, name: str) -> None:
    """A process restart: the incarnation dies, a fresh one boots from disk."""
    member = cluster.members[name]
    member.task.cancel()
    try:
        await member.task
    except asyncio.CancelledError:
        pass
    _boot(cluster, name, member.storage)


def leader_now(cluster: Cluster) -> str | None:
    """Whoever claims leadership in the cluster's highest term, if anyone."""
    top = max(member.node.term for member in cluster.members.values())
    for member in cluster.members.values():
        if member.node.role == LEADER and member.node.term == top:
            return member.name
    return None


async def wait_for_leader(
    cluster: Cluster, *, timeout_s: float = 10.0, settle_s: float = 1.5
) -> str:
    """The name of a leader in the cluster's highest term, once the claim holds.

    A leader cut off from its followers goes on calling itself one until a
    higher term reaches it, so a single reading can name a leader the cluster
    has already abandoned. A majority that stops hearing heartbeats opens a new
    term within one election timeout (the node's window is 0.45s-0.9s), and that
    higher term retires the stale leader from ``leader_now`` for good -- so a
    reading that still holds after ``settle_s`` names a leader the cluster is
    following. Callers churning leadership on purpose can shorten the window.
    """
    async with asyncio.timeout(timeout_s):
        while True:
            leader = leader_now(cluster)
            if leader is None:
                await asyncio.sleep(0.05)
                continue
            await asyncio.sleep(settle_s)
            if leader_now(cluster) == leader:
                return leader
