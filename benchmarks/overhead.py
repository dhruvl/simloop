"""Compare scheduling overhead: SimLoop against the stock asyncio loop.

The workload passes a token around a ring of queue-connected tasks, so the
measured cost is almost purely task switching and queue hand-off. Each loop
gets one warmup run, then ``--repeats`` measured runs on a fresh loop, taken
in turns so a laptop that heats up or gets busy slows every loop alike. Wall
clock and CPU time are both reported, median and range: they disagree, and the
disagreement is the finding. If uvloop is importable it gets a row too
(``uv run --with uvloop python benchmarks/overhead.py``); simloop does not
depend on it. Run with
``python benchmarks/overhead.py [--tasks N] [--rounds M] [--repeats K]``.
SimLoop's number includes trace recording — that is the price of
replayability.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
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


def _run_once(
    make_loop: Callable[[], asyncio.AbstractEventLoop], n_tasks: int, rounds: int
) -> tuple[float, float]:
    loop = make_loop()
    wall, cpu = time.perf_counter(), time.process_time()
    try:
        loop.run_until_complete(_token_ring(n_tasks, rounds))
    finally:
        loop.close()
    return time.perf_counter() - wall, time.process_time() - cpu


def _loops() -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    loops: dict[str, Callable[[], asyncio.AbstractEventLoop]] = {
        "asyncio": asyncio.new_event_loop,
    }
    try:
        uvloop = importlib.import_module("uvloop")
    except ImportError:
        pass
    else:
        loops["uvloop"] = uvloop.new_event_loop
    loops["SimLoop"] = lambda: SimLoop(seed=0)
    return loops


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure SimLoop scheduling overhead vs stock asyncio."
    )
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=9)
    args = parser.parse_args()
    hops = args.tasks * args.rounds
    loops = _loops()

    for make_loop in loops.values():  # warmup, dropped
        _run_once(make_loop, args.tasks, args.rounds)
    walls: dict[str, list[float]] = {name: [] for name in loops}
    cpus: dict[str, list[float]] = {name: [] for name in loops}
    for _ in range(args.repeats):
        for name, make_loop in loops.items():
            wall, cpu = _run_once(make_loop, args.tasks, args.rounds)
            walls[name].append(wall)
            cpus[name].append(cpu)

    def per_hop(times: list[float]) -> str:
        median, low, high = (
            t / hops * 1e6 for t in (statistics.median(times), min(times), max(times))
        )
        return f"{median:>6.2f} ({low:.2f}-{high:.2f})"

    print(
        f"{args.tasks} tasks x {args.rounds} rounds = {hops} hops, "
        f"{args.repeats} interleaved runs, us/hop median (range)"
    )
    print(f"{'loop':<10}{'wall':>20}{'cpu':>20}")
    for name in loops:
        print(f"{name:<10}{per_hop(walls[name]):>20}{per_hop(cpus[name]):>20}")
    stock = statistics.median(walls["asyncio"])
    print(f"SimLoop wall vs asyncio: {statistics.median(walls['SimLoop']) / stock:.2f}x")


if __name__ == "__main__":
    main()
