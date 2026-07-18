"""A stateless worker: lease, execute, heartbeat, commit, complete.

The worker holds no durable state, so crash recovery is simply "start
another worker". Correctness rests on the fencing token it carries: the
store refuses stale tokens, and a stale commit tells the worker its lease
was superseded, at which point it walks away without completing.
"""

from __future__ import annotations

import asyncio
from typing import Any

from jobqueue import wire
from jobqueue.store import EffectStore


class Worker:
    def __init__(
        self,
        worker_id: str,
        store: EffectStore,
        *,
        broker_host: str = "broker",
        broker_port: int = 7000,
        renew: bool = True,
        wait_s: float = 1.0,
        rpc_timeout_s: float = 1.0,
        renew_every_s: float = 0.5,
        retries: int = 4,
        retry_base_s: float = 0.25,
    ) -> None:
        self._worker_id = worker_id
        self._store = store
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._renew = renew
        self._wait_s = wait_s
        self._rpc_timeout_s = rpc_timeout_s
        self._renew_every_s = renew_every_s
        self._retries = retries
        self._retry_base_s = retry_base_s

    async def run(self) -> None:
        while True:
            lease = await self._rpc(
                {
                    "op": "acquire",
                    "worker_id": self._worker_id,
                    "wait_s": self._wait_s,
                }
            )
            if lease is None or lease.get("job_id") is None:
                continue
            await self._work(lease)

    async def _work(self, lease: dict[str, Any]) -> None:
        job_id: str = lease["job_id"]
        token: int = lease["token"]
        payload: dict[str, Any] = lease["payload"]
        if not self._store.begin(job_id, token):
            return  # a newer lease exists; don't even start
        renew_task: asyncio.Task[None] | None = None
        if self._renew:
            renew_task = asyncio.get_running_loop().create_task(
                self._renew_forever(job_id, token)
            )
        try:
            try:
                await self._execute(payload)
            except Exception:
                return  # poison: abandon and let the lease expire
            if self._store.commit(job_id, token, payload["value"]) == "stale":
                return  # superseded mid-run; the new holder owns completion
            await self._rpc({"op": "complete", "job_id": job_id, "token": token})
        finally:
            if renew_task is not None:
                renew_task.cancel()

    async def _execute(self, payload: dict[str, Any]) -> None:
        await asyncio.sleep(payload["duration"])
        if payload["poison"]:
            raise RuntimeError(f"poison job {payload['value']!r}")

    async def _renew_forever(self, job_id: str, token: int) -> None:
        while True:
            await asyncio.sleep(self._renew_every_s)
            await self._rpc({"op": "renew", "job_id": job_id, "token": token})

    async def _rpc(self, message: dict[str, Any]) -> dict[str, Any] | None:
        for attempt in range(self._retries):
            response = await wire.call(
                self._broker_host,
                self._broker_port,
                message,
                timeout_s=self._rpc_timeout_s,
            )
            if response is not None:
                return response
            await asyncio.sleep(self._retry_base_s * 2**attempt)
        return None
