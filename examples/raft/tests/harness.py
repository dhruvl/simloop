"""Cluster assembly and fault choreography shared across the raft suites."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from simloop import SimLoop, sim

from raft import wire
from raft.node import LEADER, PORT, Event, RaftNode, Safeguards
from raft.storage import Entry, MemoryStorage

from checks import check_invariants


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


async def chaos(cluster: Cluster, rng: random.Random) -> None:
    """A seed-derived fault schedule: partition windows and process restarts.

    Cut sizes never exceed half the cluster, so a quorum side always exists
    and the driver -- which is never partitioned -- can keep proposing.
    """
    loop = sim_loop()
    for _ in range(3):
        await asyncio.sleep(rng.uniform(0.5, 2.0))
        cut = rng.sample(cluster.names, rng.randint(1, (len(cluster.names) - 1) // 2))
        rest = [name for name in cluster.names if name not in cut]
        loop.net.partition(cut, rest)
        await asyncio.sleep(rng.uniform(0.5, 3.0))
        loop.net.heal()
        if rng.random() < 0.5:
            await restart(cluster, rng.choice(cluster.names))


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


def applied_anywhere(cluster: Cluster, command: str) -> bool:
    return any(
        any(entry.command == command for entry in member.node.applied)
        for member in cluster.members.values()
    )


async def propose(cluster: Cluster, command: str, *, timeout_s: float = 30.0) -> None:
    """Submit to whoever leads until the command lands in an applied log.

    At-least-once: if a leader accepts the command and is then deposed
    before committing, the retry can commit it twice under different
    indices. The invariants don't mind -- client-session dedupe is one of
    the things this demo honestly does not implement.
    """
    async with asyncio.timeout(timeout_s):
        while True:
            for name in cluster.names:
                reply = await wire.call(
                    name, PORT, {"op": "propose", "command": command}, timeout_s=0.5
                )
                if reply is not None and reply.get("ok"):
                    for _ in range(10):  # give the commit a moment before resubmitting
                        if applied_anywhere(cluster, command):
                            return
                        await asyncio.sleep(0.2)
            if applied_anywhere(cluster, command):
                return
            await asyncio.sleep(0.2)


async def settle(cluster: Cluster, *, timeout_s: float = 120.0) -> None:
    """Wait (in virtual time) until every live member applied the same sequence."""
    async with asyncio.timeout(timeout_s):
        while True:
            for member in cluster.members.values():
                # A node that died of a real bug would otherwise just drop out
                # of the quorum below and let the rest converge without it.
                # Reading the result re-raises whatever killed it. Restarts
                # cancel their task, and a cancelled task has no result to
                # speak of, so those stay excluded quietly as before.
                if member.task.done() and not member.task.cancelled():
                    member.task.result()
            live = [
                member.node
                for member in cluster.members.values()
                if not member.task.done()
            ]
            applied = [node.applied for node in live]
            if applied and applied[0] and all(a == applied[0] for a in applied):
                return
            await asyncio.sleep(0.2)


async def figure_eight(safeguards: Safeguards) -> Cluster:
    """The paper's Figure 8: an old-term entry reaches a quorum much later.

    With the commit gate on, that entry may only commit once an entry of
    the sitting leader's own term commits above it; with the gate off, a
    counting leader commits it directly -- and a rival with a later-term
    log can still erase it.
    """
    loop = sim_loop()
    cluster = await start_cluster(size=5, safeguards=safeguards)
    s1 = await wait_for_leader(cluster)
    await propose(cluster, "a")
    others = [name for name in cluster.names if name != s1]
    buddy = others[0]
    # "b" lands on s1 and buddy only, then the pair is cut off.
    loop.net.partition([s1, buddy], others[1:])
    await wire.call(s1, PORT, {"op": "propose", "command": "b"}, timeout_s=1.0)
    await asyncio.sleep(0.5)
    # The majority elects a new leader; it takes "c" and is cut before
    # replicating it anywhere (on the seeds where the race lands that way).
    s5 = await wait_for_leader(cluster, timeout_s=30.0, settle_s=1.0)
    await wire.call(s5, PORT, {"op": "propose", "command": "c"}, timeout_s=1.0)
    loop.net.heal()
    loop.net.partition([s5], [name for name in cluster.names if name != s5])
    # s1's side can now retake the cluster and spread "b" to a quorum. The
    # two followers s5 left behind keep timing out and campaigning, and each
    # doomed run -- their logs are short of s1's, so the up-to-date check
    # refuses them -- drags the term up before s1 can win one of its own. The
    # window has to cover those rounds plus the catch-up.
    await asyncio.sleep(5.0)
    loop.net.heal()
    loop.net.partition([s1], [name for name in cluster.names if name != s1])
    # With s1 gone and s5 back, s5's later-term log can win and erase "b".
    # A settle() here would only ever time out: the gate is exercisable only
    # with the no-op off, and s1 -- still cut away -- never catches up.
    await propose(cluster, "d", timeout_s=60.0)
    await asyncio.sleep(2.0)
    return cluster


def verify(cluster: Cluster) -> None:
    """Hold the run's whole history against the four safety claims."""
    check_invariants(cluster.logs(), cluster.events)
