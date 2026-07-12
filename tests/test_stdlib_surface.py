"""The stdlib coordination surface the docs promise works on SimLoop."""

import asyncio

import pytest

from simloop import SimLoop


def test_gather_and_taskgroup_run_deterministically() -> None:
    async def double(x: int) -> int:
        await asyncio.sleep(0.01)
        return 2 * x

    async def main() -> list[int]:
        gathered = await asyncio.gather(double(1), double(2), double(3))
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(double(x)) for x in (4, 5)]
        return list(gathered) + [task.result() for task in tasks]

    loop = SimLoop(seed=0)
    try:
        assert loop.run_until_complete(main()) == [2, 4, 6, 8, 10]
    finally:
        loop.close()


def test_timeout_uses_virtual_time() -> None:
    async def main() -> None:
        async with asyncio.timeout(10.0):
            await asyncio.sleep(1.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert loop.time() == 1.0


def test_wait_for_times_out_in_virtual_time() -> None:
    async def main() -> None:
        await asyncio.wait_for(asyncio.sleep(60.0), timeout=1.0)

    loop = SimLoop(seed=0)
    try:
        with pytest.raises(TimeoutError):
            loop.run_until_complete(main())
    finally:
        loop.close()
    assert loop.time() == 1.0


def test_event_lock_and_semaphore_coordinate_tasks() -> None:
    order: list[str] = []

    async def main() -> None:
        event = asyncio.Event()
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(1)

        async def waiter() -> None:
            await event.wait()
            async with lock, semaphore:
                order.append("waiter")

        task = asyncio.create_task(waiter())
        async with lock:
            order.append("main")
            event.set()
            await asyncio.sleep(0.01)
        await task

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert order == ["main", "waiter"]


def test_queue_coordinates_producer_and_consumer() -> None:
    async def main() -> list[int]:
        queue: asyncio.Queue[int] = asyncio.Queue()
        received: list[int] = []

        async def producer() -> None:
            for value in range(3):
                await queue.put(value)
                await asyncio.sleep(0.01)

        async def consumer() -> None:
            for _ in range(3):
                received.append(await queue.get())

        async with asyncio.TaskGroup() as group:
            group.create_task(producer())
            group.create_task(consumer())
        return received

    loop = SimLoop(seed=0)
    try:
        assert loop.run_until_complete(main()) == [0, 1, 2]
    finally:
        loop.close()
