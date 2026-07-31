"""Search seeds in worker processes without changing what gets reported.

Parallelism must not change the answer. Sequential exploration stops at the
first failing seed, so a parallel search has to report that same seed and not
whichever worker happened to finish first — the batch frontier below is what
enforces that, by treating a failure as an answer only once every batch that
could hold an earlier one has finished.

Workers are told which seeds to run and answer with an index and two
strings. Traces, choice logs and reports never cross a process boundary:
the parent rebuilds the report by re-running the one seed that matters,
which doubles as a check that replay held across processes at all.

This module is plain library code, like the rest of the package: it imports
no pytest, and the multiprocessing machinery is imported lazily so that
``import simloop`` does not pay for a feature most runs never use.
"""

from __future__ import annotations

import functools
import importlib.util
import inspect
import pickle
import sys
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, NamedTuple, cast

from simloop._loop import SimLoop
from simloop._run import Workload, finish, run_once

# Seeds handed out per work item. Small enough that a failure early in the
# range is not stuck behind a long batch, large enough that the hand-off
# across processes is not what the run spends its time on.
BATCH_SIZE = 16

_CoroutineFn = Callable[[], Coroutine[Any, Any, object]]


class Failure(NamedTuple):
    """A seed that failed, as reported from wherever it ran.

    ``at`` is the seed's position in the list the search was given, not its
    value: positions are what order the search by, and a seed list is not
    required to be ascending or contiguous. ``exc_type`` and ``message``
    are all that is kept of the exception, which stays in the process that
    raised it.
    """

    at: int
    exc_type: str
    message: str


class ParallelDeterminismError(RuntimeError):
    """A seed did not do the same thing in a worker and in the parent."""


def require_picklable(fn: Workload) -> None:
    """Refuse a workload that cannot reach a worker process.

    Checked up front, because the alternative is a pickling error thrown
    from inside the pool — possibly a long way into a sweep — in a
    traceback that says nothing about which workload it was or why the
    constraint exists.
    """
    try:
        pickle.dumps(fn)
    except Exception as exc:
        raise TypeError(
            "exploring with jobs>1 sends the workload to worker processes, "
            "so it must be picklable: a module-level function, or a "
            "functools.partial of one. Lambdas, closures and functions "
            "defined inside another function are sequential-only "
            f"(pickling raised {type(exc).__name__}: {exc})"
        ) from exc


def check_reproduced(
    seed: int, worker: Failure | None, parent: Exception | None
) -> None:
    """Confirm ``seed`` did in this process what it did in a worker.

    Compared by exception type: a message can legitimately carry values
    that a repr moves around, but the type is the failure's identity. A
    mismatch is never papered over — it means the same seed took two
    different paths in two processes, which makes every parallel result
    suspect until it is explained.
    """
    worker_type = worker.exc_type if worker is not None else None
    parent_type = type(parent).__name__ if parent is not None else None
    if worker_type == parent_type:
        return
    there = _outcome(worker_type, worker.message if worker is not None else "")
    here = _outcome(parent_type, str(parent) if parent is not None else "")
    raise ParallelDeterminismError(
        f"seed {seed} {there} in a worker process but {here} when re-run in "
        "this one: replay did not hold across processes for this workload. "
        "Re-run the same seeds with jobs=1; until this is explained, no "
        "parallel result from this workload means anything."
    )


def _outcome(exc_type: str | None, message: str) -> str:
    if exc_type is None:
        return "passed"
    return f"raised {exc_type}: {message}" if message else f"raised {exc_type}"


def find_lowest_failure(
    fn: Workload,
    seeds: Sequence[int],
    *,
    jobs: int,
    batch_size: int | None = None,
) -> Failure | None:
    """Run ``seeds`` across ``jobs`` processes; report the earliest failure.

    Earliest by position in ``seeds``, which is the seed sequential
    exploration would have stopped at. Batches go out in order and come
    back in whatever order they finish, so the search runs until no batch
    that could hold an earlier failure is left — never stopping at the
    first failure to arrive.
    """
    # Imported here rather than at module scope: ProcessPoolExecutor drags in
    # multiprocessing, and importing simloop should not cost that.
    import multiprocessing
    from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait

    require_picklable(fn)
    size = batch_size if batch_size is not None else _batch_size(len(seeds), jobs)
    frontier = _Frontier(_batches(len(seeds), size))
    # spawn explicitly: fork is unsafe in a process that has threads, and a
    # worker that re-imports cleanly is the same worker Windows would get.
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=jobs, mp_context=context) as pool:
        running: set[Future[Failure | None]] = set()
        while True:
            while len(running) < jobs:
                batch = frontier.take()
                if batch is None:
                    break
                block = tuple(seeds[position] for position in batch)
                running.add(pool.submit(_run_batch, fn, batch.start, block))
            if not running:
                return frontier.failure
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            running -= done
            for future in done:
                frontier.record(future.result())


def _batch_size(total: int, jobs: int) -> int:
    """Seeds per batch: capped, and small enough for several turns each.

    The cap is what keeps a failure early in the range from waiting on a
    long batch. The division is what keeps a short range from being cut
    into one batch per worker, where a single slow seed leaves every other
    worker idle with nothing left to hand out.
    """
    return max(1, min(BATCH_SIZE, total // (jobs * 4)))


def _batches(total: int, size: int) -> list[range]:
    """Cut ``total`` seed positions into ascending batches of ``size``."""
    return [range(start, min(start + size, total)) for start in range(0, total, size)]


class _Frontier:
    """Hands batches out in order and holds the earliest failure so far.

    Batches are handed out in ascending order, so once one has failed at
    position ``p``, every batch not yet handed out starts after ``p`` and
    cannot beat it: the frontier stops there rather than spending workers
    on seeds sequential exploration would never have reached. Batches
    already running can still beat it, so recording keeps the minimum
    rather than the first answer to arrive.
    """

    def __init__(self, batches: Sequence[range]) -> None:
        self._batches = list(batches)
        self._next = 0
        self.failure: Failure | None = None

    def take(self) -> range | None:
        """The next batch still worth running, or ``None`` if there is none."""
        if self._next == len(self._batches):
            return None
        batch = self._batches[self._next]
        if self.failure is not None and batch.start > self.failure.at:
            return None
        self._next += 1
        return batch

    def record(self, failure: Failure | None) -> None:
        """Take in what one finished batch found."""
        if failure is None:
            return
        if self.failure is None or failure.at < self.failure.at:
            self.failure = failure


def _run_batch(fn: Workload, start: int, seeds: Sequence[int]) -> Failure | None:
    """Run ``seeds`` in order and report the first that failed.

    This is what runs in a worker. Everything a report is built from — the
    trace, the choice log, the pending tasks — is produced here and thrown
    away, because the parent re-runs the seed that matters anyway and
    shipping any of it back would mean pickling it.
    """
    for offset, seed in enumerate(seeds):
        loop = SimLoop(seed)
        try:
            failure = run_once(loop, fn)
        finally:
            finish(loop)
        if failure is not None:
            return Failure(start + offset, type(failure).__name__, str(failure))
    return None


@dataclass(frozen=True, slots=True)
class ModuleWorkload:
    """A workload named by where it lives, so that it can be pickled.

    A ``sim_test`` wrapper closes over the coroutine function it decorates,
    and closures do not pickle, so a worker cannot be handed the workload
    itself. It can be handed the address: a file to import and a name to
    look up in it.
    """

    path: str
    name: str

    def __call__(self) -> Coroutine[Any, Any, object]:
        return _coroutine_function(self.path, self.name)()


@functools.lru_cache(maxsize=None)
def _coroutine_function(path: str, name: str) -> _CoroutineFn:
    """Import ``path`` and find the coroutine function ``name`` stands for.

    The lookup lands on the ``sim_test`` wrapper, which would explore a
    whole seed range of its own; unwrapping the ``__wrapped__`` link
    ``functools.wraps`` left on it gets back the single-run coroutine
    function the explorer wants. Cached because a worker asks once per
    seed and the import is the expensive half.
    """
    module = _import_file(path)
    found = getattr(module, name, None)
    if found is None:
        raise LookupError(f"{path} defines no {name!r} for a worker to run")
    return cast(_CoroutineFn, inspect.unwrap(found))


def _import_file(path: str) -> ModuleType:
    """Import a module from its file, the way pytest reaches a test file."""
    name = Path(path).stem
    loaded = sys.modules.get(name)
    if loaded is not None and getattr(loaded, "__file__", None) == path:
        return loaded
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"no way to import {path} in a worker process")
    module = importlib.util.module_from_spec(spec)
    # Registered before execution so that anything the module defines can be
    # found by name later, which is what pickling its objects needs.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
