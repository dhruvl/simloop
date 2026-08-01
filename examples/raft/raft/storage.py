"""What a Raft peer must remember across restarts, and the disks it can keep it on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


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

    def sync(self) -> None: ...


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

    def sync(self) -> None:
        """Nothing to do: a save here is already as durable as this disk gets."""


class Disk(Protocol):
    """The slice of a host's storage this demo uses: a mapping that flushes."""

    def __contains__(self, key: object) -> bool: ...

    def __getitem__(self, key: str) -> Any: ...

    def __setitem__(self, key: str, value: Any) -> None: ...

    def sync(self) -> None: ...


class DiskStorage:
    """Persistent state on a machine's disk, made durable when ``sync`` says so.

    The whole record goes down under one key, as a single immutable value.
    That is what makes a save atomic against a power cut: a disk that keeps
    a prefix of what it was holding can rewind the node to an earlier
    record, but never leave it with this term beside the previous vote.
    """

    KEY = "raft-state"

    def __init__(self, disk: Disk) -> None:
        self._disk = disk

    def load(self) -> PersistentState:
        if self.KEY not in self._disk:
            return PersistentState()
        term, voted_for, log = self._disk[self.KEY]
        return PersistentState(
            term=term,
            voted_for=voted_for,
            log=[Entry(entry_term, command) for entry_term, command in log],
        )

    def save(self, state: PersistentState) -> None:
        self._disk[self.KEY] = (
            state.term,
            state.voted_for,
            tuple((entry.term, entry.command) for entry in state.log),
        )

    def sync(self) -> None:
        self._disk.sync()
