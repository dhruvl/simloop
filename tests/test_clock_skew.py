"""Per-host clocks that read differently while true time stays shared."""

from __future__ import annotations

import asyncio

import pytest

from simloop import SimLoop


def _network() -> SimLoop:
    loop = SimLoop(seed=0)
    loop.net.host("broker")
    loop.net.host("worker")
    return loop


def test_each_host_reads_its_own_clock() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=2.5)

    async def read_on(host: str) -> float:
        reading: asyncio.Task[float] = loop.net.host(host).create_task(_read())
        return await reading

    async def _read() -> float:
        return loop.time()

    async def main() -> tuple[float, float, float]:
        return (
            await read_on("broker"),
            await read_on("worker"),
            loop.time(),  # driver, unskewed
        )

    broker, worker, driver = loop.run_until_complete(main())
    loop.close()
    assert worker == pytest.approx(broker + 2.5)
    assert driver == pytest.approx(broker)


def test_clock_offset_reads_back_what_was_set() -> None:
    loop = _network()
    assert loop.net.clock_offset("worker") == 0.0
    loop.net.set_clock("worker", offset=-1.5)
    assert loop.net.clock_offset("worker") == -1.5
    assert loop.net.clock_offset("broker") == 0.0
    loop.close()


def test_durations_are_immune_to_offset() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=100.0)

    async def timed_sleep() -> float:
        before = loop.time()
        await asyncio.sleep(1.0)
        return loop.time() - before

    async def timed_timeout() -> float:
        before = loop.time()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1.0):
                await asyncio.sleep(10.0)
        return loop.time() - before

    async def timed_wait_for() -> float:
        before = loop.time()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.sleep(10.0), 1.0)
        return loop.time() - before

    async def main() -> tuple[float, float, float]:
        host = loop.net.host("worker")
        slept = await host.create_task(timed_sleep())
        timed = await host.create_task(timed_timeout())
        waited = await host.create_task(timed_wait_for())
        return slept, timed, waited

    slept, timed, waited = loop.run_until_complete(main())
    loop.close()
    assert slept == pytest.approx(1.0)
    assert timed == pytest.approx(1.0)
    assert waited == pytest.approx(1.0)


def test_call_at_means_the_callers_clock() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=10.0)
    fired_at: list[float] = []

    async def schedule() -> None:
        # "at 11 on my clock" = at 1 true
        loop.call_at(loop.time() + 1.0, lambda: fired_at.append(loop.time()))
        await asyncio.sleep(2.0)

    loop.run_until_complete(loop.net.host("worker").create_task(schedule()))
    loop.close()
    # The run lasts two true seconds, so firing at all is what pins the
    # deadline to one true second away: read as eleven true seconds, the
    # callback would never have run.
    assert len(fired_at) == 1
    # The callback carries the scheduling task's context (asyncio copies the
    # current context into the handle), so it reads the worker's clock as
    # well: it sees exactly the deadline it was given.
    assert fired_at[0] == pytest.approx(11.0)


def test_call_later_ignores_the_offset() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=10.0)
    fired_at: list[float] = []

    async def schedule() -> None:
        loop.call_later(1.0, lambda: fired_at.append(loop.time()))
        await asyncio.sleep(2.0)

    loop.run_until_complete(loop.net.host("worker").create_task(schedule()))
    loop.close()
    # A delay is a duration: one true second later, which the worker reads
    # as 11.0 because its clock runs ten seconds fast.
    assert fired_at == [pytest.approx(11.0)]


def test_a_deadline_from_the_driver_stays_true() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=10.0)
    fired_at: list[float] = []

    async def main() -> None:
        # Scheduled from the driver, so 1.0 means 1.0 true.
        loop.call_at(1.0, lambda: fired_at.append(loop.time()))
        await asyncio.sleep(2.0)

    loop.run_until_complete(main())
    loop.close()
    assert fired_at == [pytest.approx(1.0)]


def test_hosts_disagree_about_lease_expiry() -> None:
    loop = _network()
    loop.net.set_clock("worker", offset=2.0)

    async def broker_grants() -> float:
        return loop.time() + 1.0  # lease valid one second, broker clock

    async def worker_checks(expiry: float) -> bool:
        return loop.time() < expiry

    async def main() -> bool:
        granting: asyncio.Task[float] = loop.net.host("broker").create_task(
            broker_grants()
        )
        expiry = await granting
        checking: asyncio.Task[bool] = loop.net.host("worker").create_task(
            worker_checks(expiry)
        )
        return await checking

    holds = loop.run_until_complete(main())
    loop.close()
    assert not holds  # the worker's fast clock already sees it expired


def test_set_clock_validates() -> None:
    loop = _network()
    with pytest.raises(OSError):
        loop.net.set_clock("ghost", offset=1.0)
    with pytest.raises(OSError):
        loop.net.clock_offset("ghost")
    loop.close()


def test_zero_skew_traces_match_a_loop_that_never_heard_of_skew() -> None:
    def run(configure: bool) -> str:
        loop = _network()
        if configure:
            loop.net.set_clock("worker", offset=0.0)

        async def main() -> None:
            async def _echo(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                w.write(await r.readline())
                w.close()

            async def serve() -> None:
                server = await asyncio.start_server(_echo, "0.0.0.0", 9000)
                async with server:
                    await asyncio.sleep(0.5)

            async def ask() -> None:
                reader, writer = await asyncio.open_connection("broker", 9000)
                writer.write(b"x\n")
                await reader.readline()
                writer.close()

            serving = loop.net.host("broker").create_task(serve())
            await asyncio.sleep(0.01)
            await loop.net.host("worker").create_task(ask())
            await serving

        try:
            loop.run_until_complete(main())
            return loop.trace_hash()
        finally:
            loop.close()

    assert run(False) == run(True)


def test_skew_leaves_the_trace_on_the_true_clock() -> None:
    def run(offset: float) -> str:
        loop = _network()
        loop.net.set_clock("worker", offset=offset)

        async def work() -> None:
            await asyncio.sleep(1.0)
            async with asyncio.timeout(2.0):
                await asyncio.sleep(0.5)

        loop.run_until_complete(loop.net.host("worker").create_task(work()))
        try:
            return loop.trace_hash()
        finally:
            loop.close()

    # Durations and trace timestamps are both true-clock, so a skewed run
    # traces byte-identically to an unskewed one.
    assert run(0.0) == run(3600.0) == run(-3600.0)
