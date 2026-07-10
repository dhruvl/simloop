"""A toy client/server exchange used to prove scheduling determinism.

Three clients talk to one server over in-memory queues, with sleeps that
collide at identical virtual deadlines so several tasks are frequently
runnable at once. The exchange is written against the plain asyncio API;
nothing in it knows it is running on a simulated loop.
"""

import asyncio

from simloop import SimLoop

_CLIENTS = ("alice", "bob", "carol")
_REQUESTS_EACH = 3

_Request = tuple[str, int, "asyncio.Queue[str]"]


async def _serve(requests: "asyncio.Queue[_Request]", total: int) -> None:
    for _ in range(total):
        name, number, inbox = await requests.get()
        await asyncio.sleep(0.005)
        await inbox.put(f"{name}:{number}:done")


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


def _run(seed: int) -> tuple[str, dict[str, list[str]]]:
    loop = SimLoop(seed)
    try:
        replies = loop.run_until_complete(_exchange())
    finally:
        loop.close()
    return loop.trace_hash(), replies


def test_same_seed_gives_identical_traces() -> None:
    for seed in range(5):
        hashes = {_run(seed)[0] for _ in range(20)}
        assert len(hashes) == 1, f"seed {seed} produced diverging traces"


def test_different_seeds_give_different_orderings() -> None:
    hashes = {_run(seed)[0] for seed in range(10)}
    assert len(hashes) >= 8


def test_every_seed_delivers_correct_replies() -> None:
    expected = {
        name: [f"{name}:{number}:done" for number in range(_REQUESTS_EACH)]
        for name in _CLIENTS
    }
    for seed in range(10):
        assert _run(seed)[1] == expected
