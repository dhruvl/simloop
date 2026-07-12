"""Compare scheduling overhead: SimLoop against the stock asyncio loop.

The workload passes a token around a ring of queue-connected tasks, so the
measured cost is almost purely task switching and queue hand-off. Run with
``python benchmarks/overhead.py [--tasks N] [--rounds M]``. SimLoop's number
includes trace recording — that is the honest price of replayability.
"""

from __future__ import annotations

import argparse
import asyncio
import time

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


def _measure(loop: asyncio.AbstractEventLoop, n_tasks: int, rounds: int) -> float:
    start = time.perf_counter()
    try:
        loop.run_until_complete(_token_ring(n_tasks, rounds))
    finally:
        loop.close()
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure SimLoop scheduling overhead vs stock asyncio."
    )
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=200)
    args = parser.parse_args()
    hops = args.tasks * args.rounds

    stock = _measure(asyncio.new_event_loop(), args.tasks, args.rounds)
    simulated = _measure(SimLoop(seed=0), args.tasks, args.rounds)

    print(f"{args.tasks} tasks x {args.rounds} rounds = {hops} hops")
    print(f"{'loop':<10}{'total s':>10}{'us/hop':>10}")
    for label, seconds in (("asyncio", stock), ("SimLoop", simulated)):
        print(f"{label:<10}{seconds:>10.4f}{seconds / hops * 1e6:>10.2f}")
    print(f"overhead: {simulated / stock:.2f}x")


if __name__ == "__main__":
    main()
