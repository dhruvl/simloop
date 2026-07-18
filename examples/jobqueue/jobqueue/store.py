"""The demo's stand-in for fenced external storage.

Real systems make exactly-once true at the storage boundary: writes carry a
fencing token and the store rejects tokens older than the newest it has
seen, and applying the same logical effect twice is a no-op. This class
plays that role for the whole cluster. Both checks can be switched off so
the test suite can demonstrate what each one prevents.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Commit:
    """One accepted effect. ``stale`` marks a write from a superseded lease."""

    job_id: str
    token: int
    value: str
    stale: bool


class EffectStore:
    def __init__(self, *, fenced: bool = True, idempotent: bool = True) -> None:
        self.fenced = fenced
        self.idempotent = idempotent
        self.begun: list[tuple[str, int]] = []
        self.commits: list[Commit] = []
        self.rejected: list[tuple[str, int, str]] = []
        self._newest: dict[str, int] = {}
        self._committed: set[str] = set()

    def begin(self, job_id: str, token: int) -> bool:
        """Announce a token before working; False means the lease is stale."""
        newest = self._newest.get(job_id, 0)
        if self.fenced and token < newest:
            self.rejected.append((job_id, token, "stale-begin"))
            return False
        self._newest[job_id] = max(newest, token)
        self.begun.append((job_id, token))
        return True

    def commit(self, job_id: str, token: int, value: str) -> str:
        newest = self._newest.get(job_id, 0)
        if self.fenced and token < newest:
            self.rejected.append((job_id, token, "stale"))
            return "stale"
        if self.idempotent and job_id in self._committed:
            self.rejected.append((job_id, token, "duplicate"))
            return "duplicate"
        self._newest[job_id] = max(newest, token)
        self._committed.add(job_id)
        self.commits.append(
            Commit(job_id=job_id, token=token, value=value, stale=token < newest)
        )
        return "ok"
