"""Leader elections under the simulated network."""

from __future__ import annotations

import asyncio

from simloop import sim_test

from raft.node import LEADER

import harness


@sim_test
async def test_a_quiet_cluster_elects_exactly_one_leader() -> None:
    cluster = await harness.start_cluster()
    leader = await harness.wait_for_leader(cluster)
    await asyncio.sleep(2.0)
    leaders = [m.name for m in cluster.members.values() if m.node.role == LEADER]
    assert leaders == [leader]


@sim_test
async def test_a_settled_leader_stays_leader() -> None:
    cluster = await harness.start_cluster()
    leader = await harness.wait_for_leader(cluster)
    term = cluster.members[leader].node.term
    await asyncio.sleep(5.0)
    assert cluster.members[leader].node.role == LEADER
    assert cluster.members[leader].node.term == term


@sim_test
async def test_the_cluster_survives_a_leader_restart() -> None:
    cluster = await harness.start_cluster()
    await harness.wait_for_leader(cluster)
    first = await harness.wait_for_leader(cluster)
    before = cluster.members[first].node.term
    await harness.restart(cluster, first)
    leader = await harness.wait_for_leader(cluster)
    assert cluster.members[leader].node.role == LEADER
    # The restart cut the heartbeats, so the survivors must have opened a term.
    assert cluster.members[leader].node.term > before


@sim_test
async def test_a_partitioned_leader_steps_down_on_heal() -> None:
    cluster = await harness.start_cluster()
    loop = harness.sim_loop()
    first = await harness.wait_for_leader(cluster)
    rest = [name for name in cluster.names if name != first]
    loop.net.partition([first], rest)
    second = await harness.wait_for_leader(cluster)
    assert second != first
    loop.net.heal()
    await asyncio.sleep(2.0)
    assert cluster.members[first].node.role != LEADER
