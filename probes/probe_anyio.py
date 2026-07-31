"""Probe: anyio's asyncio backend driven from inside a simulated run.

anyio is the structured-concurrency layer under httpx and starlette, so
whether its asyncio backend needs anything beyond tasks, futures and timers
decides how much of that stack can be simulated at all.
"""

from __future__ import annotations

from simloop import SimLoop

LIBRARY = "anyio"
DISTRIBUTION = "anyio"
TIER = 1
NOTES = "Asyncio backend only; nothing here touches a socket."


async def probe(loop: SimLoop) -> str:
    # Imported inside the probe so the module stays importable — and its
    # contract testable — without the probes dependency group installed.
    import anyio

    send, receive = anyio.create_memory_object_stream[str](max_buffer_size=1)
    received: list[str] = []

    async def produce() -> None:
        async with send:
            for word in ("one", "two", "three"):
                await anyio.sleep(0.25)
                await send.send(word)

    async def consume() -> None:
        async with receive:
            async for word in receive:
                received.append(word)

    async with anyio.create_task_group() as group:
        group.start_soon(produce)
        group.start_soon(consume)

    with anyio.move_on_after(1.0):
        await anyio.sleep(5.0)

    return (
        f"task group, memory object stream ({', '.join(received)}), "
        f"anyio.sleep and move_on_after; virtual clock reached {loop.time()}s"
    )
