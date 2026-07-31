"""Log replication: ordering, catch-up, durability, and rollback."""

from __future__ import annotations

import asyncio

from simloop import sim_test

from raft import wire
from raft.node import PORT

import harness


@sim_test
async def test_commands_apply_everywhere_in_order() -> None:
    cluster = await harness.start_cluster()
    await harness.wait_for_leader(cluster)
    for i in range(3):
        await harness.propose(cluster, f"k{i}")
    await harness.settle(cluster)
    logs = [member.node.applied for member in cluster.members.values()]
    assert all(log == logs[0] for log in logs)
    assert [entry.command for entry in logs[0]] == ["k0", "k1", "k2"]


@sim_test
async def test_a_lagging_follower_catches_up() -> None:
    cluster = await harness.start_cluster()
    loop = harness.sim_loop()
    leader = await harness.wait_for_leader(cluster)
    behind = next(name for name in cluster.names if name != leader)
    loop.net.partition([behind], [name for name in cluster.names if name != behind])
    for i in range(3):
        await harness.propose(cluster, f"k{i}")
    loop.net.heal()
    await harness.propose(cluster, "after")
    await harness.settle(cluster)
    assert (
        cluster.members[behind].node.applied
        == cluster.members[leader].node.applied
    )


@sim_test
async def test_committed_entries_survive_rolling_restarts() -> None:
    cluster = await harness.start_cluster()
    await harness.wait_for_leader(cluster)
    await harness.propose(cluster, "durable")
    for name in list(cluster.names):
        await harness.restart(cluster, name)
        await asyncio.sleep(1.5)
    await harness.wait_for_leader(cluster)
    await harness.propose(cluster, "after")
    await harness.settle(cluster)
    for member in cluster.members.values():
        assert any(entry.command == "durable" for entry in member.node.applied)


@sim_test
async def test_a_deposed_leaders_unshared_entries_vanish() -> None:
    cluster = await harness.start_cluster()
    loop = harness.sim_loop()
    first = await harness.wait_for_leader(cluster)
    await harness.propose(cluster, "keep")
    rest = [name for name in cluster.names if name != first]
    loop.net.partition([first], rest)
    await wire.call(first, PORT, {"op": "propose", "command": "lost"}, timeout_s=1.0)
    await harness.wait_for_leader(cluster)
    await harness.propose(cluster, "win")
    loop.net.heal()
    await harness.propose(cluster, "after")
    await harness.settle(cluster)
    for member in cluster.members.values():
        commands = [entry.command for entry in member.node.applied]
        assert "lost" not in commands
        assert "keep" in commands
        assert "win" in commands
