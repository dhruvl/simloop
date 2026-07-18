"""Whole-cluster scenarios under seed exploration."""

from __future__ import annotations

from simloop import sim_test

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
