"""Sweep the jobqueue example at campaign scale and report what it found.

Three questions, one script. ``green`` asks whether the intact jobqueue
survives a hundred thousand seeds of chaos — a sweep long enough that it has
to survive being killed and resumed, hence the checkpoint file. ``ablations``
asks how *often* a broken jobqueue gets caught: not the first failing seed,
which the test suite already pins, but every failing seed in the range, which
turns "the explorer finds this bug" into a density. ``stability`` asks
whether those failing seeds still replay identically when run a hundred more
times, which is the claim the whole tool rests on.

Everything here is measurement, not testing: the assertions live in
examples/jobqueue/tests, and this script must never become the place a bug
is discovered but not recorded. A failing seed in ``green`` is therefore
printed in full and exits nonzero — an intact jobqueue failing under chaos is
the most interesting thing this script could possibly find.

Run from the repository root::

    uv run python benchmarks/campaign.py green [--seeds N] [--jobs J]
    uv run python benchmarks/campaign.py ablations [--seeds N] [--jobs J]
    uv run python benchmarks/campaign.py stability --checkpoint FILE

Both sweeping subcommands checkpoint after every chunk and take ``--resume``
to pick up where a killed run stopped.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import os
import sys
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "jobqueue"
# Done at import time rather than in main(), because a spawned worker
# re-imports this module to reach the scenario it was handed and needs the
# same path set up before the imports below run. examples/jobqueue/conftest.py
# does the same job for pytest.
for _entry in (_EXAMPLE, _EXAMPLE / "tests"):
    if str(_entry) not in sys.path:
        sys.path.insert(0, str(_entry))

from simloop import SimLoop, explore, sim  # noqa: E402
from simloop._run import Workload, finish, run_once  # noqa: E402

import helpers  # noqa: E402
from invariants import InvariantViolation  # noqa: E402

# Seeds per checkpoint for the green sweep. Large enough that starting a
# worker pool is noise against the seeds it then runs, small enough that a
# killed run loses a minute rather than an hour.
GREEN_CHUNK = 5000
# Seeds per chunk for the ablations. Much smaller, because a chunk here is
# also the unit of parallelism: an ablation whose seeds nearly all fail is
# scanned one seed at a time within a chunk, so the cores have to be fed by
# having many chunks in flight rather than by exploring one range in
# parallel.
ABLATION_CHUNK = 250
# Trace events kept on a green failure's report. Generous: this report is the
# only thing a killed campaign leaves behind about a real bug.
GREEN_TRACE_TAIL = 60
# Unexpected-failure lines printed before the rest are counted instead. A
# whole ablation can go wrong at once, and ten thousand near-identical lines
# would bury the summary that says which ablation it was.
_UNEXPECTED_SHOWN = 10


# ---------------------------------------------------------------------------
# Scenarios
#
# Thin re-declarations of what the example's tests already run. The
# authoritative copies live in examples/jobqueue/tests/test_campaign.py and
# examples/jobqueue/tests/test_mutations.py, which this script deliberately
# does not import from: those bodies are closures inside test functions, and a
# closure cannot be pickled to a worker process. Everything with any substance
# to it — the cluster assembly, the fault choreography, the invariants — is
# imported from helpers, so what is duplicated is only the wiring.
# ---------------------------------------------------------------------------


async def chaos_campaign() -> None:
    """3 workers, 2 clients, 8 jobs, randomized faults, every invariant."""
    loop = helpers.sim_loop()
    rng = sim.random
    cluster = await helpers.start_cluster(workers=3, clients=2)
    submits = []
    for i, client in enumerate(cluster.clients):
        host = loop.net.host(f"c{i + 1}")
        for j in range(4):
            submits.append(
                host.create_task(
                    client.submit(
                        f"c{i + 1}.{j}",
                        duration=rng.uniform(0.05, 1.0),
                        poison=rng.random() < 0.15,
                    )
                )
            )
    chaos_task = loop.create_task(helpers.chaos(cluster, rng))
    job_ids = [await task for task in submits]
    assert all(job_id is not None for job_id in job_ids)
    await helpers.settle(cluster, timeout_s=600.0)
    chaos_task.cancel()
    helpers.verify(cluster)


async def unfenced_store() -> None:
    """Store fencing off: a zombie's stale commit is accepted."""
    cluster = await helpers.zombie_run(helpers.EffectStore(fenced=False))
    helpers.verify(cluster)


async def unidempotent_store() -> None:
    """Idempotent apply off: a crash between commit and complete doubles it."""
    store = helpers.CrashOnFirstCommit("w1", idempotent=False)
    cluster = await helpers.crash_after_commit_run(store)
    helpers.verify(cluster)


async def broker_fencing_off() -> None:
    """Broker fencing off, paired with a fully ablated store."""
    cluster = await helpers.zombie_run(
        helpers.EffectStore(fenced=False, idempotent=False),
        broker=helpers.Broker(fencing=False),
    )
    helpers.verify(cluster)


async def no_idempotency_key() -> None:
    """Client idempotency key off: a lost ack duplicates the submit."""
    loop = helpers.sim_loop()
    cluster = await helpers.start_cluster(
        workers=1, clients=1, client_kwargs={"idempotency": False}
    )
    submit_task = loop.net.host("c1").create_task(
        cluster.clients[0].submit("d", duration=0.1)
    )
    await asyncio.sleep(0.12)
    loop.net.partition(["c1"], ["broker"])
    await asyncio.sleep(1.5)
    loop.net.heal()
    job_id = await submit_task
    assert job_id is not None
    await helpers.settle(cluster)
    helpers.verify(cluster)


async def unbounded_attempts() -> None:
    """Attempt cap off: a poison job never converges."""
    loop = helpers.sim_loop()
    broker = helpers.Broker(max_attempts=None, lease_s=0.5, backoff_base_s=0.1)
    cluster = await helpers.start_cluster(workers=1, clients=1, broker=broker)
    job_id = await loop.net.host("c1").create_task(
        cluster.clients[0].submit("p", duration=0.05, poison=True)
    )
    assert job_id is not None
    await helpers.settle(cluster, timeout_s=30.0)


async def renew_off_unidempotent() -> None:
    """Lease renewal off, paired with a non-idempotent store: double commit."""
    store = helpers.EffectStore(idempotent=False)
    cluster = await helpers.slow_job_run(store)
    helpers.verify(cluster)


@dataclass(frozen=True)
class Ablation:
    """One safeguard switched off, and what turning it off must produce.

    ``expected`` names the invariants the example's tests assert for this
    ablation, by the label :func:`_label` gives a failure. Anything else is
    reported loudly: a mutation that starts failing for a new reason is
    either a change in the example or a bug in the harness, and neither is
    something a density number should quietly average over.
    """

    name: str
    scenario: Workload
    expected: tuple[str, ...]


ABLATIONS: tuple[Ablation, ...] = (
    Ablation("unfenced-store", unfenced_store, ("no-zombie-writes",)),
    Ablation("unidempotent-store", unidempotent_store, ("exactly-once",)),
    Ablation(
        "broker-fencing-off",
        broker_fencing_off,
        ("exactly-once", "no-zombie-writes"),
    ),
    # Campaign-scale finding: on roughly 2 seeds in 10,000 the duplicate
    # accepted without a key is still queued when the cluster settles, so the
    # violation surfaces as convergence rather than exactly-once. The 200-seed
    # test budget never reaches those seeds; both flavors are the same missing
    # safeguard.
    Ablation(
        "no-idempotency-key",
        no_idempotency_key,
        ("exactly-once", "convergence"),
    ),
    Ablation("unbounded-attempts", unbounded_attempts, ("TimeoutError",)),
    Ablation(
        "renew-off-unidempotent", renew_off_unidempotent, ("exactly-once",)
    ),
)

ABLATION_BY_NAME = {ablation.name: ablation for ablation in ABLATIONS}


def _label(exc: BaseException) -> str:
    """What a failure is, in one word a table column can hold."""
    if isinstance(exc, InvariantViolation):
        return exc.invariant
    return type(exc).__name__


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


def _load(path: Path, kind: str, settings: dict[str, int]) -> dict[str, Any]:
    """Read a checkpoint to resume from, refusing one from a different run.

    Resuming into a checkpoint written with other settings would silently
    produce a summary describing neither run, so the settings that change
    which seeds get explored are compared rather than trusted.
    """
    state: dict[str, Any] = json.loads(path.read_text())
    if state.get("kind") != kind:
        raise SystemExit(
            f"{path} holds a {state.get('kind')!r} campaign, not {kind!r}"
        )
    for key, value in settings.items():
        if state.get(key) != value:
            raise SystemExit(
                f"{path} was written with {key}={state.get(key)}, and this "
                f"run has {key}={value}: resuming would mix two campaigns"
            )
    return state


def _save(path: Path, state: dict[str, Any]) -> None:
    """Write the checkpoint whole, so a kill cannot leave half a file."""
    scratch = path.with_suffix(path.suffix + ".partial")
    scratch.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    scratch.replace(path)


def _chunks(seeds: int, size: int) -> list[tuple[int, int]]:
    """Cut ``seeds`` into ascending half-open [start, stop) ranges."""
    return [(start, min(start + size, seeds)) for start in range(0, seeds, size)]


def _duration(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.1f} s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.2f} h"


def _rate(seeds: int, seconds: float) -> float:
    return seeds / seconds if seconds > 0 else 0.0


# ---------------------------------------------------------------------------
# green
# ---------------------------------------------------------------------------


def _run_green(args: argparse.Namespace) -> int:
    path = Path(args.checkpoint)
    settings = {"seeds": args.seeds, "chunk": args.chunk}
    chunks = _chunks(args.seeds, args.chunk)
    done = 0
    elapsed = 0.0
    if args.resume and path.exists():
        state = _load(path, "green", settings)
        done = int(state["chunks_done"])
        elapsed = float(state["elapsed_s"])
        print(
            f"resuming from {path}: {done} of {len(chunks)} chunks already "
            f"clear ({_duration(elapsed)} spent so far)"
        )
    elif args.resume:
        print(f"no checkpoint at {path} yet: starting from seed 0")

    print(
        f"green: {args.seeds:,} seeds of jobqueue chaos, {args.jobs} jobs, "
        f"{args.chunk:,}-seed chunks"
    )
    for index in range(done, len(chunks)):
        low, high = chunks[index]
        chunk_started = time.perf_counter()
        report = explore(
            chaos_campaign,
            range(low, high),
            jobs=args.jobs,
            trace_tail=GREEN_TRACE_TAIL,
        )
        chunk_time = time.perf_counter() - chunk_started
        if report is not None:
            elapsed += chunk_time
            _save(
                path,
                {
                    "kind": "green",
                    **settings,
                    "jobs": args.jobs,
                    "chunks_done": index,
                    "seeds_done": low + report.seeds_passed + 1,
                    "elapsed_s": round(elapsed, 3),
                    "failure": {
                        "seed": report.seed,
                        "label": _label(report.exception),
                        "message": str(report.exception),
                    },
                },
            )
            print()
            print(f"FAILURE: seed {report.seed} broke an intact jobqueue")
            print(report.render())
            print()
            print(
                "This is a real bug, not a flake: re-run that one seed with "
                f"explore(chaos_campaign, [{report.seed}])."
            )
            return 1
        elapsed += chunk_time
        seeds_done = high
        _save(
            path,
            {
                "kind": "green",
                **settings,
                "jobs": args.jobs,
                "chunks_done": index + 1,
                "seeds_done": seeds_done,
                "elapsed_s": round(elapsed, 3),
                "failure": None,
            },
        )
        print(
            f"seeds {low:,}-{high - 1:,} clear in {_duration(chunk_time)} "
            f"({_rate(high - low, chunk_time):.1f} seeds/s); "
            f"{seeds_done:,}/{args.seeds:,} done"
        )

    print()
    print("--- summary (for benchmarks/README.md) ---")
    print(f"seeds:        {args.seeds:,}")
    print(f"jobs:         {args.jobs}")
    print(f"wall time:    {_duration(elapsed)}")
    print(f"throughput:   {_rate(args.seeds, elapsed):.1f} seeds/s")
    print("result:       green, no invariant violated")
    return 0


# ---------------------------------------------------------------------------
# ablations
# ---------------------------------------------------------------------------


def _scan(name: str, low: int, high: int) -> list[tuple[int, str, str]]:
    """Every failing seed in ``[low, high)``, as (seed, label, message).

    Runs in a worker process under ``--jobs``. Exploration is sequential
    here — the parallelism is across chunks — and restarts one seed past
    each failure, which is what makes this count failures instead of
    stopping at the first one. Nothing but strings and ints crosses back:
    the exception stays in the process that raised it, as everywhere else in
    this package.
    """
    scenario = ABLATION_BY_NAME[name].scenario
    found: list[tuple[int, str, str]] = []
    seed = low
    while seed < high:
        report = explore(scenario, range(seed, high), trace_tail=0)
        if report is None:
            break
        found.append((report.seed, _label(report.exception), str(report.exception)))
        seed = report.seed + 1
    return found


def _scanned(
    name: str, chunks: Sequence[tuple[int, int]], jobs: int
) -> Iterator[list[tuple[int, str, str]]]:
    """Scan ``chunks``, yielding their findings in ascending seed order.

    Ordered results from unordered work: chunks run concurrently, but a
    chunk's findings are yielded only when every earlier chunk has been
    yielded, so the progress lines and the recorded failure list read the
    same on any number of cores.
    """
    if jobs == 1:
        for low, high in chunks:
            yield _scan(name, low, high)
        return
    from concurrent.futures import ProcessPoolExecutor

    # spawn for the same reason the library uses it: fork is unsafe once a
    # process has threads, and a worker that re-imports cleanly is the one
    # Windows would give us anyway.
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=jobs, mp_context=context) as pool:
        yield from pool.map(
            _scan,
            [name] * len(chunks),
            [low for low, _ in chunks],
            [high for _, high in chunks],
        )


def _run_ablations(args: argparse.Namespace) -> int:
    path = Path(args.checkpoint)
    settings = {"seeds": args.seeds, "chunk": args.chunk}
    chunks = _chunks(args.seeds, args.chunk)
    results: dict[str, Any] = {}
    if args.resume and path.exists():
        state = _load(path, "ablations", settings)
        results = dict(state["ablations"])
        print(
            f"resuming from {path}: "
            f"{sum(1 for r in results.values() if r['chunks_done'] == len(chunks))}"
            f" of {len(ABLATIONS)} ablations already complete"
        )
    elif args.resume:
        print(f"no checkpoint at {path} yet: starting from the first ablation")

    print(
        f"ablations: {len(ABLATIONS)} mutations x {args.seeds:,} seeds, "
        f"{args.jobs} jobs, {args.chunk:,}-seed chunks"
    )
    unexpected: list[str] = []
    for ablation in ABLATIONS:
        record = results.setdefault(
            ablation.name,
            {"chunks_done": 0, "elapsed_s": 0.0, "failing_seeds": [], "labels": {}},
        )
        done = int(record["chunks_done"])
        if done == len(chunks):
            print(f"{ablation.name}: already complete, skipping")
            continue
        remaining = chunks[done:]
        started = time.perf_counter()
        scanned = _scanned(ablation.name, remaining, args.jobs)
        for offset, found in enumerate(scanned):
            low, high = remaining[offset]
            for seed, label, message in found:
                record["failing_seeds"].append(seed)
                # One example message per label, from its lowest seed: ten
                # thousand copies of the same sentence would be the bulk of
                # the checkpoint file and say nothing the first one did not.
                bucket = record["labels"].setdefault(
                    label, {"count": 0, "first_seed": seed, "example": message}
                )
                bucket["count"] += 1
                if label not in ablation.expected:
                    unexpected.append(
                        f"{ablation.name} seed {seed} failed with {label!r}, "
                        f"not {' or '.join(ablation.expected)}: {message}"
                    )
            record["chunks_done"] = done + offset + 1
            record["elapsed_s"] = round(
                float(record["elapsed_s"]) + time.perf_counter() - started, 3
            )
            started = time.perf_counter()
            _save(
                path,
                {
                    "kind": "ablations",
                    **settings,
                    "jobs": args.jobs,
                    "ablations": results,
                },
            )
            print(
                f"{ablation.name}: seeds {low:,}-{high - 1:,} scanned, "
                f"{len(found)} failing "
                f"({len(record['failing_seeds'])} so far in {high:,} seeds)"
            )

    print()
    print("--- summary (for benchmarks/README.md) ---")
    print(
        f"{'ablation':<24}{'failures':>10}{'per 1k':>9}"
        f"{'first':>8}  invariant"
    )
    total = 0.0
    for ablation in ABLATIONS:
        record = results[ablation.name]
        seeds = record["failing_seeds"]
        total += float(record["elapsed_s"])
        labels = sorted(record["labels"])
        first = seeds[0] if seeds else "-"
        print(
            f"{ablation.name:<24}{len(seeds):>10,}"
            f"{len(seeds) / args.seeds * 1000:>9.1f}"
            f"{first:>8}  {', '.join(labels) or '-'}"
        )
    print(f"seeds per ablation: {args.seeds:,}")
    print(f"wall time: {_duration(total)} for {len(ABLATIONS) * args.seeds:,} runs")
    print(f"failing seeds recorded in {path}")

    if unexpected:
        print()
        print(f"UNEXPECTED FAILURES ({len(unexpected)}):")
        for line in unexpected[:_UNEXPECTED_SHOWN]:
            print(f"  {line}")
        hidden = len(unexpected) - _UNEXPECTED_SHOWN
        if hidden > 0:
            print(f"  ... and {hidden:,} more")
        print(
            "An ablation failing for a reason its test does not claim means "
            "the example or the harness changed under the campaign."
        )
        return 1
    empty = [
        ablation.name
        for ablation in ABLATIONS
        if not results[ablation.name]["failing_seeds"]
    ]
    if empty:
        print()
        print(f"NO FAILURES FOUND for: {', '.join(empty)}")
        print("An ablation the explorer no longer catches is a regression.")
        return 1
    return 0


# ---------------------------------------------------------------------------
# stability
# ---------------------------------------------------------------------------


def _replay(scenario: Workload, seed: int) -> tuple[str, str]:
    """Run one seed and report its trace hash and how it ended.

    The hash is read before teardown, like everywhere else: teardown
    schedules callbacks of its own, and they would land in the trace.
    """
    loop = SimLoop(seed)
    try:
        failure = run_once(loop, scenario)
        return loop.trace_hash(), _label(failure) if failure else "passed"
    finally:
        finish(loop)


def _sample(
    results: dict[str, Any], count: int
) -> list[tuple[str, int]]:
    """Pick ``count`` failing seeds spread evenly across the ablations.

    Even across ablations first, then even within each one, taking the
    midpoint of each slice rather than its start: a sample drawn from the
    front of every list would only ever re-run the low seeds the example's
    tests already cover.
    """
    stocked = [
        (name, list(results[name]["failing_seeds"]))
        for name in (ablation.name for ablation in ABLATIONS)
        if name in results and results[name]["failing_seeds"]
    ]
    if not stocked:
        return []
    picked: list[tuple[str, int]] = []
    for index, (name, seeds) in enumerate(stocked):
        quota = count // len(stocked) + (1 if index < count % len(stocked) else 0)
        quota = min(quota, len(seeds))
        for step in range(quota):
            picked.append((name, seeds[(2 * step + 1) * len(seeds) // (2 * quota)]))
    return picked


def _run_stability(args: argparse.Namespace) -> int:
    path = Path(args.checkpoint)
    state: dict[str, Any] = json.loads(path.read_text())
    if state.get("kind") != "ablations":
        raise SystemExit(f"{path} is not an ablations checkpoint")
    sample = _sample(state["ablations"], args.sample)
    if not sample:
        raise SystemExit(f"{path} records no failing seeds to re-run")
    unknown = sorted({name for name, _ in sample} - set(ABLATION_BY_NAME))
    if unknown:
        raise SystemExit(
            f"{path} names ablations this script does not define: {unknown}"
        )

    print(
        f"stability: {len(sample)} failing seeds from {path}, "
        f"{args.reruns} runs each"
    )
    started = time.perf_counter()
    diverged: list[str] = []
    for name, seed in sample:
        scenario = ABLATION_BY_NAME[name].scenario
        outcomes = {_replay(scenario, seed) for _ in range(args.reruns)}
        if len(outcomes) == 1:
            (trace_hash, label) = outcomes.pop()
            print(
                f"{name} seed {seed}: stable across {args.reruns} runs "
                f"({label}, trace {trace_hash[:12]})"
            )
            continue
        diverged.append(name + f" seed {seed}")
        print(f"{name} seed {seed}: DIVERGED across {args.reruns} runs")
        for trace_hash, label in sorted(outcomes):
            print(f"  {label}, trace {trace_hash[:12]}")
    elapsed = time.perf_counter() - started

    print()
    print("--- summary (for benchmarks/README.md) ---")
    spread = len({name for name, _ in sample})
    print(f"seeds sampled: {len(sample)} across {spread} ablations")
    print(f"runs per seed: {args.reruns}")
    print(f"total runs:    {len(sample) * args.reruns:,} in {_duration(elapsed)}")
    if diverged:
        print(f"result:        {len(diverged)} DIVERGED: {', '.join(diverged)}")
        print()
        print(
            "A seed that does not replay identically makes every report this "
            "tool prints suspect. Do not record a number from this run."
        )
        return 1
    print("result:        identical trace hash on every run")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Campaign sweeps of the jobqueue example.",
    )
    jobs = os.cpu_count() or 2
    subs = parser.add_subparsers(dest="command", required=True)

    green = subs.add_parser("green", help="sweep the intact jobqueue")
    green.add_argument("--seeds", type=int, default=100_000, help="seeds to sweep")
    green.add_argument("--jobs", type=int, default=jobs, help="worker processes")
    green.add_argument(
        "--chunk", type=int, default=GREEN_CHUNK, help="seeds per checkpoint"
    )
    green.add_argument(
        "--checkpoint", default="campaign-green.json", help="progress file"
    )
    green.add_argument(
        "--resume", action="store_true", help="continue from the checkpoint"
    )
    green.set_defaults(run=_run_green)

    ablations = subs.add_parser(
        "ablations", help="count failing seeds per switched-off safeguard"
    )
    ablations.add_argument(
        "--seeds", type=int, default=10_000, help="seeds per ablation"
    )
    ablations.add_argument("--jobs", type=int, default=jobs, help="worker processes")
    ablations.add_argument(
        "--chunk",
        type=int,
        default=ABLATION_CHUNK,
        help="seeds per chunk, and the unit of parallelism",
    )
    ablations.add_argument(
        "--checkpoint", default="campaign-ablations.json", help="progress file"
    )
    ablations.add_argument(
        "--resume", action="store_true", help="continue from the checkpoint"
    )
    ablations.set_defaults(run=_run_ablations)

    stability = subs.add_parser(
        "stability", help="re-run sampled failing seeds for identical traces"
    )
    stability.add_argument(
        "--checkpoint",
        default="campaign-ablations.json",
        help="ablations checkpoint to draw failing seeds from",
    )
    stability.add_argument(
        "--sample", type=int, default=20, help="failing seeds to re-run"
    )
    stability.add_argument(
        "--reruns", type=int, default=100, help="runs per sampled seed"
    )
    stability.set_defaults(run=_run_stability)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    for name in ("seeds", "jobs", "chunk", "sample", "reruns"):
        value = getattr(args, name, None)
        if value is not None and value < 1:
            raise SystemExit(f"--{name} must be at least 1")
    run = args.run
    assert callable(run)
    result = run(args)
    assert isinstance(result, int)
    return result


if __name__ == "__main__":
    # Guarded, and it has to be: the workers are spawned, so they re-import
    # this file, and anything run at import time would run again in each of
    # them.
    sys.exit(main())
