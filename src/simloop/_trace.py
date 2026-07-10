"""Append-only record of event-loop scheduling decisions.

Two runs that made identical scheduling decisions produce identical hashes,
so a hash comparison is a cheap proof that a run was reproduced exactly.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

EventKind = Literal["schedule", "run", "advance"]


@dataclass(frozen=True, slots=True)
class TraceEvent:
    kind: EventKind
    when: float
    seq: int
    label: str


class TraceRecorder:
    def __init__(self) -> None:
        self._events: list[TraceEvent] = []

    def record(self, kind: EventKind, when: float, seq: int, label: str) -> None:
        self._events.append(TraceEvent(kind, when, seq, label))

    @property
    def events(self) -> tuple[TraceEvent, ...]:
        return tuple(self._events)

    def hash(self) -> str:
        digest = hashlib.sha256()
        for event in self._events:
            line = f"{event.kind}|{event.when!r}|{event.seq}|{event.label}\n"
            digest.update(line.encode("utf-8"))
        return digest.hexdigest()
