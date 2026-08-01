"""Append-only record of event-loop scheduling decisions.

Two runs that made identical scheduling decisions produce identical hashes,
so a hash comparison is a cheap proof that a run was reproduced exactly.
"""

from __future__ import annotations

import hashlib
from typing import Literal, NamedTuple

EventKind = Literal["schedule", "run", "advance", "cancel", "net"]


class TraceEvent(NamedTuple):
    """One decision, immutable, named.

    A named tuple rather than a frozen dataclass because one of these is built
    for every callback the simulation ever schedules and runs: a frozen
    dataclass assigns each field through ``object.__setattr__``, which costs
    more than twice as much per event and is paid on the hottest path in the
    package. Immutability, attribute access and equality are unchanged.
    """

    kind: EventKind
    when: float
    seq: int
    label: str
    # The simulated machine whose code this event belongs to, for the
    # scheduling kinds that have one. Empty for events that belong to the
    # simulation rather than to a machine: a clock advance is global, a packet
    # event belongs to a link whose ends its label already names, and the
    # network's own delivery step belongs to the wire. No registered host can
    # be named "" (SimNetwork.host rejects an empty name), so it is never
    # mistakable for one.
    host: str = ""


class TraceRecorder:
    def __init__(self) -> None:
        self._events: list[TraceEvent] = []

    def record(
        self, kind: EventKind, when: float, seq: int, label: str, host: str = ""
    ) -> None:
        self._events.append(TraceEvent(kind, when, seq, label, host))

    def __len__(self) -> int:
        # How many events so far, without building the snapshot tuple that
        # ``events`` returns: callers that only need the count ask this on
        # every step of a run.
        return len(self._events)

    @property
    def events(self) -> tuple[TraceEvent, ...]:
        return tuple(self._events)

    def hash(self) -> str:
        digest = hashlib.sha256()
        for event in self._events:
            # Labels are qualified callback names or network labels built from
            # validated host names, and a host field is a validated host name
            # itself, so neither can contain the "|" field separator or a
            # newline (host names reject both — _net.py:33). That keeps this
            # delimiter-based serialization injective: every field is
            # recoverable from the line, so two distinct event streams can
            # never collide onto the same byte sequence.
            line = (
                f"{event.kind}|{event.when!r}|{event.seq}|{event.host}|{event.label}\n"
            )
            digest.update(line.encode("utf-8"))
        return digest.hexdigest()
