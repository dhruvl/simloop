"""Broker state machine, driven directly through handle() on a bare SimLoop."""

from __future__ import annotations

import asyncio
from typing import Any

from simloop import SimLoop

from jobqueue.broker import Broker


def _run(coro: Any) -> Any:
    loop = SimLoop(seed=0)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_submit_dedupes_on_idempotency_key() -> None:
    async def main() -> tuple[dict[str, Any], dict[str, Any], int]:
        broker = Broker()
        first = await broker.handle(
            {"op": "submit", "key": "k1", "payload": {"value": "v"}}
        )
        second = await broker.handle(
            {"op": "submit", "key": "k1", "payload": {"value": "v"}}
        )
        return first, second, len(broker.snapshot())

    first, second, jobs = _run(main())
    assert first["job_id"] == second["job_id"]
    assert jobs == 1


def test_lease_expiry_requeues_after_backoff() -> None:
    async def main() -> list[Any]:
        broker = Broker(lease_s=1.0, backoff_base_s=0.5, max_attempts=3)
        submit = await broker.handle(
            {"op": "submit", "key": "k", "payload": {"value": "v"}}
        )
        job_id = submit["job_id"]
        lease = await broker.handle(
            {"op": "acquire", "worker_id": "w", "wait_s": 0.1}
        )
        seen = [lease["token"]]
        await asyncio.sleep(1.2)  # expiry at 1.0; requeued, ready at 1.5
        seen.append(broker.snapshot()[job_id])
        early = await broker.handle(
            {"op": "acquire", "worker_id": "w", "wait_s": 0.1}
        )
        seen.append(early["job_id"])  # still backing off: nothing to lease
        await asyncio.sleep(0.5)
        again = await broker.handle(
            {"op": "acquire", "worker_id": "w", "wait_s": 0.1}
        )
        seen.append(again["token"])
        return seen

    token1, state, early_job, token2 = _run(main())
    assert token1 == 1
    assert state == ("queued", 1)
    assert early_job is None
    assert token2 == 2


def test_stale_tokens_are_rejected_and_valid_complete_lands() -> None:
    async def main() -> list[Any]:
        broker = Broker(lease_s=1.0, backoff_base_s=0.1)
        submit = await broker.handle(
            {"op": "submit", "key": "k", "payload": {"value": "v"}}
        )
        job_id = submit["job_id"]
        await broker.handle({"op": "acquire", "worker_id": "w1", "wait_s": 0.1})
        await asyncio.sleep(1.3)  # token-1 lease expires; requeued, ready at 1.1
        lease2 = await broker.handle(
            {"op": "acquire", "worker_id": "w2", "wait_s": 0.1}
        )
        results = [lease2["token"]]
        results.append(
            (await broker.handle({"op": "renew", "job_id": job_id, "token": 1}))["ok"]
        )
        results.append(
            (await broker.handle({"op": "complete", "job_id": job_id, "token": 1}))["ok"]
        )
        results.append(
            (await broker.handle({"op": "complete", "job_id": job_id, "token": 2}))["ok"]
        )
        results.append(
            (await broker.handle({"op": "complete", "job_id": job_id, "token": 2}))["ok"]
        )
        results.append(broker.snapshot()[job_id][0])
        return results

    token2, stale_renew, stale_complete, complete, again, state = _run(main())
    assert token2 == 2
    assert stale_renew is False
    assert stale_complete is False
    assert complete is True
    assert again is True  # completing a done job is idempotent
    assert state == "done"


def test_renew_extends_the_lease() -> None:
    async def main() -> tuple[str, str]:
        broker = Broker(lease_s=1.0, backoff_base_s=0.1)
        submit = await broker.handle(
            {"op": "submit", "key": "k", "payload": {"value": "v"}}
        )
        job_id = submit["job_id"]
        await broker.handle({"op": "acquire", "worker_id": "w", "wait_s": 0.1})
        await asyncio.sleep(0.8)
        renewed = await broker.handle(
            {"op": "renew", "job_id": job_id, "token": 1}
        )
        assert renewed["ok"] is True
        await asyncio.sleep(0.4)  # t=1.2: past the original expiry, inside renewal
        alive = broker.snapshot()[job_id][0]
        await asyncio.sleep(0.8)  # t=2.0: renewed lease (0.8+1.0) has expired
        return alive, broker.snapshot()[job_id][0]

    alive, expired = _run(main())
    assert alive == "leased"
    assert expired == "queued"


def test_attempts_exhaustion_dead_letters_the_job() -> None:
    async def main() -> tuple[str, int]:
        broker = Broker(lease_s=0.5, backoff_base_s=0.1, max_attempts=2)
        submit = await broker.handle(
            {"op": "submit", "key": "k", "payload": {"value": "v"}}
        )
        job_id = submit["job_id"]
        await broker.handle({"op": "acquire", "worker_id": "w", "wait_s": 0.1})
        await asyncio.sleep(0.7)  # first lease expires; ready again at 0.6
        await broker.handle({"op": "acquire", "worker_id": "w", "wait_s": 0.1})
        await asyncio.sleep(0.7)  # second lease expires; attempts == max
        return broker.snapshot()[job_id][0], broker.snapshot()[job_id][1]

    state, attempts = _run(main())
    assert state == "dead"
    assert attempts == 2


def test_long_poll_wakes_on_submit() -> None:
    async def main() -> tuple[Any, float]:
        loop = asyncio.get_running_loop()
        broker = Broker()
        waiter = loop.create_task(
            broker.handle({"op": "acquire", "worker_id": "w", "wait_s": 5.0})
        )
        await asyncio.sleep(0.5)
        await broker.handle({"op": "submit", "key": "k", "payload": {"value": "v"}})
        lease = await waiter
        return lease, loop.time()

    lease, elapsed = _run(main())
    assert lease["job_id"] is not None
    assert elapsed < 1.0  # woke on submit, not at the 5s wait cap


def test_status_reports_states() -> None:
    async def main() -> tuple[str | None, str | None]:
        broker = Broker()
        submit = await broker.handle(
            {"op": "submit", "key": "k", "payload": {"value": "v"}}
        )
        known = (await broker.handle({"op": "status", "job_id": submit["job_id"]}))[
            "state"
        ]
        unknown = (await broker.handle({"op": "status", "job_id": "nope"}))["state"]
        return known, unknown

    known, unknown = _run(main())
    assert known == "queued"
    assert unknown is None
