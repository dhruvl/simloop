"""Compare scheduling overhead: SimLoop against the stock asyncio loop.

The workload passes a token around a ring of queue-connected tasks, so the
measured cost is almost purely task switching and queue hand-off. Each loop
gets one warmup run plus ``--repeats`` measured runs on a fresh loop; the
reported overhead is the ratio of medians. Run with
``python benchmarks/overhead.py [--tasks N] [--rounds M] [--repeats K]``.
SimLoop's number includes trace recording — that is the honest price of
replayability.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from collections.abc import Callable

from simloop import SimLoop


async def _token_ring(n_tasks: int, rounds: int) -> None:
    queues: list[asyncio.Queue[int]] = [asyncio.Queue() for _ in range(n_tasks)]

    async def worker(index: int) -> None:
        for _ in range(rounds):
            token = await queues[index].get()
            await queues[(index + 1) % n_tasks].put(token + 1)

    workers = [asyncio.create_task(worker(i)) for i in range(n_tasks)]
    await queues[0].put(0)
    await asyncio.gather(*workers)


def _measure(
    make_loop: Callable[[], asyncio.AbstractEventLoop],
    n_tasks: int,
    rounds: int,
    repeats: int,
) -> list[float]:
    times: list[float] = []
    for _ in range(repeats + 1):  # first run is warmup, dropped below
        loop = make_loop()
        start = time.perf_counter()
        try:
            loop.run_until_complete(_token_ring(n_tasks, rounds))
        finally:
            loop.close()
        times.append(time.perf_counter() - start)
    return times[1:]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure SimLoop scheduling overhead vs stock asyncio."
    )
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    hops = args.tasks * args.rounds

    stock = _measure(asyncio.new_event_loop, args.tasks, args.rounds, args.repeats)
    simulated = _measure(
        lambda: SimLoop(seed=0), args.tasks, args.rounds, args.repeats
    )

    print(
        f"{args.tasks} tasks x {args.rounds} rounds = {hops} hops, "
        f"median of {args.repeats} runs"
    )
    print(f"{'loop':<10}{'median s':>10}{'min s':>10}{'us/hop':>10}")
    for label, times in (("asyncio", stock), ("SimLoop", simulated)):
        median = statistics.median(times)
        print(
            f"{label:<10}{median:>10.4f}{min(times):>10.4f}"
            f"{median / hops * 1e6:>10.2f}"
        )
    overhead = statistics.median(simulated) / statistics.median(stock)
    print(f"overhead: {overhead:.2f}x")


if __name__ == "__main__":
    main()
