import asyncio
import contextvars
import gc
import time
from typing import Any, cast

import pytest

from simloop import SimLoop, SimulationDeadlockError, SimulationFenceError


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


def test_failing_background_task_surfaces_from_run() -> None:
    async def fail() -> None:
        raise ValueError("background boom")

    async def main() -> str:
        asyncio.create_task(fail())
        await asyncio.sleep(1.0)
        return "finished"

    loop = SimLoop(seed=0)
    try:
        with pytest.raises(ValueError, match="background boom"):
            loop.run_until_complete(main())
    finally:
        loop.close()


def test_pending_background_task_does_not_fail_the_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def main() -> str:
        asyncio.create_task(asyncio.sleep(100))
        await asyncio.sleep(0)
        return "finished"

    loop = SimLoop(seed=0)
    try:
        assert loop.run_until_complete(main()) == "finished"
    finally:
        loop.close()
    # Dropping the loop finalizes the still-pending task; the destroy notice
    # must land on stderr instead of failing the (successful) run.
    del loop
    gc.collect()
    assert "Task was destroyed but it is pending!" in capsys.readouterr().err


def test_main_task_exception_wins_over_background_failure() -> None:
    async def fail() -> None:
        raise KeyError("background")

    async def main() -> None:
        asyncio.create_task(fail())
        await asyncio.sleep(1.0)
        raise ValueError("main boom")

    loop = SimLoop(seed=0)
    try:
        with pytest.raises(ValueError, match="main boom"):
            loop.run_until_complete(main())
    finally:
        loop.close()


def test_unsupported_apis_are_fenced() -> None:
    loop = SimLoop(seed=0)
    try:
        with pytest.raises(SimulationFenceError, match="run_in_executor"):
            loop.run_in_executor(None, print)
        with pytest.raises(SimulationFenceError, match="supported-api"):
            loop.call_soon_threadsafe(print)
        # Callers written against the stdlib contract keep working.
        with pytest.raises(NotImplementedError):
            loop.add_signal_handler(2, print)
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


def test_cancelled_callback_is_traced_not_run() -> None:
    fired: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        handle = loop.call_soon(fired.append, "x")
        handle.cancel()
        await asyncio.sleep(1.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert fired == []
    assert "cancel" in {event.kind for event in loop.trace}


def test_cancelled_timer_is_traced() -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        timer = loop.call_later(1.0, print, "never")
        timer.cancel()
        timer.cancel()
        await asyncio.sleep(2.0)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    cancels = [event for event in loop.trace if event.kind == "cancel"]
    assert len(cancels) == 1


def test_call_at_in_the_past_runs_at_current_time() -> None:
    fired: list[float] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(5.0)
        loop.call_at(1.0, lambda: fired.append(loop.time()))
        await asyncio.sleep(0.1)

    loop = SimLoop(seed=0)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert fired == [5.0]


def test_custom_exception_handler_takes_responsibility() -> None:
    seen: list[dict[str, Any]] = []

    async def fail() -> None:
        raise ValueError("handled elsewhere")

    async def main() -> str:
        asyncio.create_task(fail())
        await asyncio.sleep(1.0)
        return "finished"

    loop = SimLoop(seed=0)
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    try:
        assert loop.run_until_complete(main()) == "finished"
    finally:
        loop.close()
    assert len(seen) == 1
    assert isinstance(seen[0].get("exception"), ValueError)


def test_broken_exception_handler_fails_the_run() -> None:
    def broken(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        raise KeyError("handler bug")

    async def fail() -> None:
        raise ValueError("original")

    async def main() -> None:
        asyncio.create_task(fail())
        await asyncio.sleep(1.0)

    loop = SimLoop(seed=0)
    loop.set_exception_handler(broken)
    try:
        with pytest.raises(KeyError, match="handler bug"):
            loop.run_until_complete(main())
    finally:
        loop.close()


def test_clearing_the_handler_restores_failing_by_default() -> None:
    async def fail() -> None:
        raise ValueError("must surface")

    async def main() -> None:
        asyncio.create_task(fail())
        await asyncio.sleep(1.0)

    loop = SimLoop(seed=0)
    handler = lambda _loop, _context: None  # noqa: E731
    loop.set_exception_handler(handler)
    assert loop.get_exception_handler() is handler
    loop.set_exception_handler(None)
    assert loop.get_exception_handler() is None
    try:
        with pytest.raises(ValueError, match="must surface"):
            loop.run_until_complete(main())
    finally:
        loop.close()


def test_non_callable_exception_handler_is_rejected() -> None:
    loop = SimLoop(seed=0)
    try:
        with pytest.raises(TypeError):
            loop.set_exception_handler(cast(Any, 42))
    finally:
        loop.close()


def test_task_factory_is_used_by_create_task() -> None:
    created: list[asyncio.Task[Any]] = []

    def factory(loop: asyncio.AbstractEventLoop, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.Task(coro, loop=loop)
        created.append(task)
        return task

    async def add(a: int, b: int) -> int:
        return a + b

    async def main() -> int:
        task = asyncio.get_running_loop().create_task(add(2, 3), name="adder")
        return await task

    loop = SimLoop(seed=0)
    loop.set_task_factory(factory)
    try:
        assert loop.get_task_factory() is factory
        assert loop.run_until_complete(main()) == 5
    finally:
        loop.close()
    assert any(task.get_name() == "adder" for task in created)


def test_task_factory_receives_context_kwarg() -> None:
    received: list[dict[str, Any]] = []

    def factory(
        loop: asyncio.AbstractEventLoop, coro: Any, **kwargs: Any
    ) -> asyncio.Task[Any]:
        received.append(kwargs)
        return asyncio.Task(coro, loop=loop, **kwargs)

    async def noop() -> None:
        return None

    async def main() -> None:
        loop = asyncio.get_running_loop()
        await loop.create_task(noop())
        await loop.create_task(noop(), context=contextvars.copy_context())

    loop = SimLoop(seed=0)
    loop.set_task_factory(factory)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    kw_sets = [set(kwargs) for kwargs in received]
    assert set() in kw_sets  # a call with no context kwarg
    assert {"context"} in kw_sets  # the context-carrying call


def test_clearing_the_task_factory_restores_default() -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    loop = SimLoop(seed=0)
    loop.set_task_factory(lambda loop, coro: asyncio.Task(coro, loop=loop))
    loop.set_task_factory(None)
    try:
        assert loop.get_task_factory() is None
        assert loop.run_until_complete(add(2, 3)) == 5
    finally:
        loop.close()


def test_non_callable_task_factory_is_rejected() -> None:
    loop = SimLoop(seed=0)
    try:
        with pytest.raises(TypeError):
            loop.set_task_factory(cast(Any, 42))
    finally:
        loop.close()
