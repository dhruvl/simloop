"""Reference client/server workload for replay-stability checks.

Importable for in-process runs; also runnable as a script —
``python tests/replay_workload.py <seed>`` prints one line:
``<trace_hash> <reply_digest>``. Jittered sleeps and UUID-tagged replies
pull the seeded shim streams into the replay proof alongside scheduling.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys

from simloop import SimLoop, sim

_CLIENTS = ("alice", "bob", "carol")
_REQUESTS_EACH = 3

_Request = tuple[str, int, "asyncio.Queue[str]"]


async def _serve(requests: "asyncio.Queue[_Request]", total: int) -> None:
    for _ in range(total):
        name, number, inbox = await requests.get()
        await asyncio.sleep(sim.random.uniform(0.001, 0.01))
        await inbox.put(f"{name}:{number}:{sim.uuid4()}")


async def _request_all(name: str, requests: "asyncio.Queue[_Request]") -> list[str]:
    inbox: asyncio.Queue[str] = asyncio.Queue()
    replies: list[str] = []
    for number in range(_REQUESTS_EACH):
        await requests.put((name, number, inbox))
        await asyncio.sleep(0.01 * (number + 1))
        replies.append(await inbox.get())
    return replies


async def _exchange() -> dict[str, list[str]]:
    requests: asyncio.Queue[_Request] = asyncio.Queue()
    server = asyncio.create_task(_serve(requests, len(_CLIENTS) * _REQUESTS_EACH))
    clients = {
        name: asyncio.create_task(_request_all(name, requests)) for name in _CLIENTS
    }
    replies = {name: await task for name, task in clients.items()}
    await server
    return replies


def run(seed: int) -> str:
    loop = SimLoop(seed)
    try:
        replies = loop.run_until_complete(_exchange())
    finally:
        loop.close()
    digest = hashlib.sha256(repr(sorted(replies.items())).encode()).hexdigest()
    return f"{loop.trace_hash()} {digest}"


if __name__ == "__main__":
    print(run(int(sys.argv[1])))
