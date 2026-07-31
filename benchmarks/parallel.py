"""Measure seed exploration throughput: one process against several.

Every seed in the range passes, so both modes explore the whole range and
the comparison is like for like — a workload that failed early would time
how quickly each mode gives up, which is a different question. The workload
is the same token ring the overhead benchmark uses, sized so that one seed
costs milliseconds rather than microseconds: seeds that run for less than
the process hand-off are dominated by it, and reporting a speedup from those
would be a measurement of the batch size.

Each mode gets one warmup run plus ``--repeats`` measured runs, and the
parallel numbers include starting and stopping the worker pool, which is
what a real sweep pays. A speedup well below the core count is the normal
result on a laptop whose cores are not all the same speed; sweeping
``--jobs`` shows where it flattens out. Run with ``python
benchmarks/parallel.py [--seeds N] [--jobs J] [--tasks N] [--rounds M]
[--repeats K]``.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import os
import statistics
import time

from simloop import explore
from simloop._run import Workload


async def _token_ring(n_tasks: int, rounds: int) -> None:
    queues: list[asyncio.Queue[int]] = [asyncio.Queue() for _ in range(n_tasks)]

    async def worker(index: int) -> None:
        for _ in range(rounds):
            token = await queues[index].get()
            await queues[(index + 1) % n_tasks].put(token + 1)

    workers = [asyncio.create_task(worker(i)) for i in range(n_tasks)]
    await queues[0].put(0)
    await asyncio.gather(*workers)


def _measure(workload: Workload, seeds: int, jobs: int, repeats: int) -> list[float]:
    times: list[float] = []
    for _ in range(repeats + 1):  # first run is warmup, dropped below
        start = time.perf_counter()
        report = explore(workload, range(seeds), jobs=jobs)
        assert report is None, "the benchmark workload must pass every seed"
        times.append(time.perf_counter() - start)
    return times[1:]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure seed exploration throughput across processes."
    )
    parser.add_argument("--seeds", type=int, default=1000)
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 2)
    parser.add_argument("--tasks", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    workload = functools.partial(_token_ring, args.tasks, args.rounds)
    sequential = _measure(workload, args.seeds, 1, args.repeats)
    parallel = _measure(workload, args.seeds, args.jobs, args.repeats)

    print(
        f"{args.seeds} seeds x ({args.tasks} tasks x {args.rounds} rounds), "
        f"median of {args.repeats} runs"
    )
    print(f"{'mode':<12}{'median s':>10}{'min s':>10}{'seeds/s':>10}")
    for label, times in (("sequential", sequential), (f"{args.jobs} jobs", parallel)):
        median = statistics.median(times)
        print(
            f"{label:<12}{median:>10.4f}{min(times):>10.4f}"
            f"{args.seeds / median:>10.1f}"
        )
    speedup = statistics.median(sequential) / statistics.median(parallel)
    print(f"speedup: {speedup:.2f}x on {os.cpu_count()} cores")


if __name__ == "__main__":
    main()
