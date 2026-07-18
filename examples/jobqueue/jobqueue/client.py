"""A submitting client: idempotency keys, timeouts, retries with backoff.

A timed-out submit is ambiguous — the broker may or may not have created
the job. The client retries with the *same* idempotency key, so the broker
can answer "that one again" instead of minting a duplicate.
"""

from __future__ import annotations

import asyncio
from typing import Any

from jobqueue import wire


class Client:
    def __init__(
        self,
        client_id: str,
        *,
        broker_host: str = "broker",
        broker_port: int = 7000,
        idempotency: bool = True,
        rpc_timeout_s: float = 1.0,
        retries: int = 6,
        retry_base_s: float = 0.25,
        poll_s: float = 0.5,
    ) -> None:
        self._client_id = client_id
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._idempotency = idempotency
        self._rpc_timeout_s = rpc_timeout_s
        self._retries = retries
        self._retry_base_s = retry_base_s
        self._poll_s = poll_s
        self._counter = 0
        self.acknowledged: dict[str, str] = {}

    async def submit(
        self, value: str, *, duration: float = 0.1, poison: bool = False
    ) -> str | None:
        """Submit one logical job; None means never acknowledged."""
        self._counter += 1
        key = f"{self._client_id}.{self._counter}"
        payload = {"value": value, "duration": duration, "poison": poison}
        for attempt in range(self._retries):
            wire_key = key if self._idempotency else f"{key}.try{attempt}"
            response = await wire.call(
                self._broker_host,
                self._broker_port,
                {"op": "submit", "key": wire_key, "payload": payload},
                timeout_s=self._rpc_timeout_s,
            )
            if response is not None:
                job_id: str = response["job_id"]
                self.acknowledged[key] = job_id
                return job_id
            await asyncio.sleep(self._retry_base_s * 2**attempt)
        return None

    async def wait(self, job_id: str) -> str:
        """Poll until the job reaches a terminal state; returns it."""
        while True:
            response = await wire.call(
                self._broker_host,
                self._broker_port,
                {"op": "status", "job_id": job_id},
                timeout_s=self._rpc_timeout_s,
            )
            if response is not None and response["state"] in ("done", "dead"):
                state: str = response["state"]
                return state
            await asyncio.sleep(self._poll_s)
