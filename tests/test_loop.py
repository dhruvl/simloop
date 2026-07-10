import asyncio
import time

import pytest

from simloop import SimLoop, SimulationDeadlockError


def test_runs_a_coroutine_to_completion() -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    loop = SimLoop(seed=0)
    try:
        assert loop.run_until_complete(add(2, 3)) == 5
    finally:
        loop.close()


def test_call_soon_resolves_awaited_future() -> None:
    async def main() -> str:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        loop.call_soon(fut.set_result, "ready")
        return await fut

    loop = SimLoop(seed=0)
    try:
        assert loop.run_until_complete(main()) == "ready"
    finally:
        loop.close()


def test_sleep_advances_virtual_time_not_wall_time() -> None:
    async def nap() -> None:
        await asyncio.sleep(3600)

    loop = SimLoop(seed=0)
    started = time.monotonic()
    try:
        loop.run_until_complete(nap())
    finally:
        loop.close()
    assert time.monotonic() - started < 1.0
    assert loop.time() == 3600.0


def test_timers_fire_in_deadline_order() -> None:
    order: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.call_later(2.0, order.append, "late")
        loop.call_later(1.0, order.append, "early")
        await asyncio.sleep(3.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert order == ["early", "late"]


def test_cancelled_timer_does_not_fire() -> None:
    fired: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        timer = loop.call_later(1.0, fired.append, "x")
        timer.cancel()
        await asyncio.sleep(2.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert fired == []


def test_blocked_future_raises_deadlock_error() -> None:
    async def wait_forever() -> None:
        await asyncio.get_running_loop().create_future()

    loop = SimLoop(seed=0)
    try:
        with pytest.raises(SimulationDeadlockError):
            loop.run_until_complete(wait_forever())
    finally:
        loop.close()


def test_unhandled_callback_exception_propagates() -> None:
    def boom() -> None:
        raise ValueError("boom")

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon(boom)
        await loop.create_future()

    loop = SimLoop(seed=0)
    try:
        with pytest.raises(ValueError, match="boom"):
            loop.run_until_complete(main())
    finally:
        loop.close()


def test_unsupported_apis_are_fenced() -> None:
    loop = SimLoop(seed=0)
    try:
        with pytest.raises(NotImplementedError):
            loop.run_in_executor(None, print)
    finally:
        loop.close()


def test_trace_is_recorded() -> None:
    async def main() -> None:
        await asyncio.sleep(1.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    kinds = {event.kind for event in loop.trace}
    assert kinds == {"schedule", "run", "advance"}
    assert len(loop.trace_hash()) == 64
