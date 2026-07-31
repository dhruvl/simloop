"""What a Raft peer must remember across restarts, and an in-memory disk."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class Entry:
    term: int
    command: str


@dataclass
class PersistentState:
    term: int = 0
    voted_for: str | None = None
    log: list[Entry] = field(default_factory=list)


class Storage(Protocol):
    def load(self) -> PersistentState: ...

    def save(self, state: PersistentState) -> None: ...


class MemoryStorage:
    """Survives a node restart within one run, the way a disk survives reboots."""

    def __init__(self) -> None:
        self._saved = PersistentState()

    def load(self) -> PersistentState:
        return PersistentState(
            term=self._saved.term,
            voted_for=self._saved.voted_for,
            log=list(self._saved.log),
        )

    def save(self, state: PersistentState) -> None:
        self._saved = PersistentState(
            term=state.term, voted_for=state.voted_for, log=list(state.log)
        )
