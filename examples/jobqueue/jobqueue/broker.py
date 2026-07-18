"""The broker: single authority for the job state machine.

States per job: queued -> leased -> done, with leased -> queued on lease
expiry and queued -> dead once the attempt budget is spent. Time is the
only failure detector — a worker that goes silent is indistinguishable
from a partitioned one, so the lease deadline decides, never the
connection.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from jobqueue import wire


@dataclass
class _Job:
    job_id: str
    key: str
    payload: dict[str, Any]
    state: str = "queued"
    attempts: int = 0
    token: int = 0
    ready_at: float = 0.0
    expiry: asyncio.TimerHandle | None = field(default=None, repr=False)


class Broker:
    def __init__(
        self,
        *,
        lease_s: float = 2.0,
        max_attempts: int | None = 3,
        backoff_base_s: float = 0.5,
        fencing: bool = True,
    ) -> None:
        self._lease_s = lease_s
        self._max_attempts = max_attempts
        self._backoff_base_s = backoff_base_s
        self._fencing = fencing
        self._jobs: dict[str, _Job] = {}
        self._by_key: dict[str, str] = {}
        self._order: list[str] = []
        self._ready = asyncio.Event()
        self._next_id = 0

    async def serve(self, port: int = 7000) -> None:
        server = await asyncio.start_server(self._connection, "0.0.0.0", port)
        async with server:
            await server.serve_forever()

    async def _connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                message = await wire.read_message(reader)
                response = await self.handle(message)
                wire.write_message(writer, response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, wire.FrameError):
            pass
        finally:
            writer.close()

    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        op = message["op"]
        if op == "submit":
            return self._submit(message["key"], message["payload"])
        if op == "acquire":
            return await self._acquire(message["worker_id"], message["wait_s"])
        if op == "renew":
            return self._renew(message["job_id"], message["token"])
        if op == "complete":
            return self._complete(message["job_id"], message["token"])
        if op == "status":
            return self._status(message["job_id"])
        return {"error": f"unknown op {op!r}"}

    def snapshot(self) -> dict[str, tuple[str, int]]:
        return {j.job_id: (j.state, j.attempts) for j in self._jobs.values()}

    def _submit(self, key: str, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = self._by_key.get(key)
        if job_id is None:
            self._next_id += 1
            job_id = f"j{self._next_id}"
            self._jobs[job_id] = _Job(job_id=job_id, key=key, payload=payload)
            self._by_key[key] = job_id
            self._order.append(job_id)
            self._ready.set()
        return {"job_id": job_id}

    async def _acquire(self, worker_id: str, wait_s: float) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while True:
            job = self._next_ready(loop.time())
            if job is not None:
                return self._lease(job)
            remaining = deadline - loop.time()
            if remaining <= 0:
                return {"job_id": None}
            self._ready.clear()
            try:
                async with asyncio.timeout(remaining):
                    await self._ready.wait()
            except TimeoutError:
                return {"job_id": None}

    def _next_ready(self, now: float) -> _Job | None:
        for job_id in self._order:
            job = self._jobs[job_id]
            if job.state == "queued" and job.ready_at <= now:
                return job
        return None

    def _lease(self, job: _Job) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        job.state = "leased"
        job.token += 1
        job.attempts += 1
        self._order.remove(job.job_id)
        job.expiry = loop.call_later(
            self._lease_s, self._expire, job.job_id, job.token
        )
        return {
            "job_id": job.job_id,
            "token": job.token,
            "payload": job.payload,
            "lease_s": self._lease_s,
        }

    def _expire(self, job_id: str, token: int) -> None:
        job = self._jobs.get(job_id)
        if job is None or job.state != "leased" or job.token != token:
            return
        if self._max_attempts is not None and job.attempts >= self._max_attempts:
            job.state = "dead"
            return
        loop = asyncio.get_running_loop()
        delay = self._backoff_base_s * 2 ** (job.attempts - 1)
        job.state = "queued"
        job.ready_at = loop.time() + delay
        self._order.append(job_id)
        loop.call_later(delay, self._ready.set)

    def _renew(self, job_id: str, token: int) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None or job.state != "leased":
            return {"ok": False}
        if self._fencing and token != job.token:
            return {"ok": False}
        if job.expiry is not None:
            job.expiry.cancel()
        loop = asyncio.get_running_loop()
        job.expiry = loop.call_later(
            self._lease_s, self._expire, job.job_id, job.token
        )
        return {"ok": True}

    def _complete(self, job_id: str, token: int) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None or job.state == "dead":
            return {"ok": False}
        if job.state == "done":
            return {"ok": True}
        if self._fencing and (job.state != "leased" or token != job.token):
            return {"ok": False}
        if job.expiry is not None:
            job.expiry.cancel()
        if job.state == "queued":
            self._order.remove(job.job_id)
        job.state = "done"
        return {"ok": True}

    def _status(self, job_id: str) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        return {"state": None if job is None else job.state}
