"""The four safety/liveness claims every intact run must satisfy."""

from __future__ import annotations

from collections import Counter

from jobqueue.broker import Broker
from jobqueue.store import EffectStore


class InvariantViolation(AssertionError):
    def __init__(self, invariant: str, detail: str) -> None:
        super().__init__(f"{invariant}: {detail}")
        self.invariant = invariant


def check_invariants(
    store: EffectStore, broker: Broker, acknowledged: dict[str, str]
) -> None:
    jobs = broker.snapshot()

    for key, job_id in acknowledged.items():
        state = jobs.get(job_id, ("missing", 0))[0]
        if state not in ("done", "dead"):
            raise InvariantViolation(
                "no-loss", f"acknowledged {key} -> {job_id} ended {state!r}"
            )

    per_job = Counter(commit.job_id for commit in store.commits)
    for job_id, count in per_job.items():
        if count > 1:
            raise InvariantViolation(
                "exactly-once", f"{job_id} committed {count} times"
            )
    per_value = Counter(commit.value for commit in store.commits)
    for value, count in per_value.items():
        if count > 1:
            raise InvariantViolation(
                "exactly-once", f"value {value!r} committed {count} times"
            )
    for job_id, (state, _) in jobs.items():
        if state == "done" and per_job[job_id] != 1:
            raise InvariantViolation(
                "exactly-once", f"{job_id} done with {per_job[job_id]} commits"
            )

    for commit in store.commits:
        if commit.stale:
            raise InvariantViolation(
                "no-zombie-writes",
                f"{commit.job_id} accepted token {commit.token} after a newer lease",
            )

    for job_id, (state, _) in jobs.items():
        if state in ("queued", "leased"):
            raise InvariantViolation("convergence", f"{job_id} still {state}")
