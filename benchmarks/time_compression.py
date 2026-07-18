"""Measure how much simulated time SimLoop covers per wall-clock second.

The workload is sleep-heavy: a fleet of tasks each ticking on its own
staggered interval, the way heartbeats, lease renewals, and retry backoffs
do in a real system. On the stock loop this would take the full simulated
duration in wall time; under SimLoop the clock jumps between timers, so the
whole thing costs only the callback processing. Run with
``python benchmarks/time_compression.py [--tasks N] [--ticks M] [--repeats K]``.
The reported ratio is simulated seconds per wall-clock second, median of
``--repeats`` runs after one warmup.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

from simloop import SimLoop


async def _tick_fleet(n_tasks: int, ticks: int) -> None:
    async def ticker(index: int) -> None:
        # Stagger intervals so timers do not all collide on the same instant.
        interval = 1.0 + index / n_tasks
        for _ in range(ticks):
            await asyncio.sleep(interval)

    await asyncio.gather(*(ticker(i) for i in range(n_tasks)))


def _measure(n_tasks: int, ticks: int, repeats: int) -> tuple[float, list[float]]:
    simulated = 0.0
    walls: list[float] = []
    for _ in range(repeats + 1):  # first run is warmup, dropped below
        loop = SimLoop(seed=0)
        start = time.perf_counter()
        try:
            loop.run_until_complete(_tick_fleet(n_tasks, ticks))
            simulated = loop.time()
        finally:
            loop.close()
        walls.append(time.perf_counter() - start)
    return simulated, walls[1:]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure simulated seconds covered per wall-clock second."
    )
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--ticks", type=int, default=3600)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    simulated, walls = _measure(args.tasks, args.ticks, args.repeats)
    median = statistics.median(walls)

    print(
        f"{args.tasks} tasks x {args.ticks} ticks, "
        f"median of {args.repeats} runs"
    )
    print(f"simulated: {simulated:>10.1f} s ({simulated / 3600:.2f} h)")
    print(f"wall:      {median:>10.4f} s (min {min(walls):.4f})")
    print(f"compression: {simulated / median:,.0f}x")


if __name__ == "__main__":
    main()
