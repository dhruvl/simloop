"""The four Raft safety claims every intact run must satisfy."""

from __future__ import annotations

from itertools import combinations

from raft.node import Event
from raft.storage import Entry


class InvariantViolation(AssertionError):
    def __init__(self, invariant: str, detail: str) -> None:
        super().__init__(f"{invariant}: {detail}")
        self.invariant = invariant


def check_invariants(
    logs: dict[str, tuple[Entry, ...]], events: list[Event]
) -> None:
    _election_safety(events)
    # Order matters: leader-completeness keeps only the first apply seen at an
    # index, which is the whole story only once state-machine safety holds.
    _state_machine_safety(events)
    _leader_completeness(events)
    _log_matching(logs)


def _election_safety(events: list[Event]) -> None:
    leaders: dict[int, set[str]] = {}
    for event in events:
        if event[0] == "leader":
            _, name, term, _ = event
            leaders.setdefault(term, set()).add(name)
    for term, names in leaders.items():
        if len(names) > 1:
            raise InvariantViolation(
                "election-safety", f"term {term} elected {sorted(names)}"
            )


def _state_machine_safety(events: list[Event]) -> None:
    applied: dict[int, tuple[int, str]] = {}
    for event in events:
        if event[0] == "apply":
            _, name, index, term, command = event
            first = applied.setdefault(index, (term, command))
            if first != (term, command):
                raise InvariantViolation(
                    "state-machine-safety",
                    f"index {index} applied as {first} and, at {name}, "
                    f"as {(term, command)}",
                )


def _leader_completeness(events: list[Event]) -> None:
    # An entry committed under a term-T leader must appear in the log of
    # every leader of a term above T. The floor records T at the moment of
    # the first apply: the committing leader's own election event always
    # precedes its applies, so the highest leader term seen so far is its
    # term. Leaders at or below the floor -- stale-term stragglers whose
    # counted votes predate the commit -- are outside the paper's claim.
    committed: dict[int, tuple[Entry, int]] = {}
    top_term = 0
    for event in events:
        if event[0] == "leader":
            _, name, term, log = event
            for index, (entry, floor) in committed.items():
                if term > floor and (index > len(log) or log[index - 1] != entry):
                    raise InvariantViolation(
                        "leader-completeness",
                        f"term-{term} leader {name} lacks committed "
                        f"entry {index}: {entry}",
                    )
            top_term = max(top_term, term)
        elif event[0] == "apply":
            _, _, index, term, command = event
            committed.setdefault(index, (Entry(term, command), top_term))


def _log_matching(logs: dict[str, tuple[Entry, ...]]) -> None:
    for a, b in combinations(sorted(logs), 2):
        log_a, log_b = logs[a], logs[b]
        for index in range(min(len(log_a), len(log_b)), 0, -1):
            if log_a[index - 1].term == log_b[index - 1].term:
                if log_a[:index] != log_b[:index]:
                    raise InvariantViolation(
                        "log-matching",
                        f"{a} and {b} agree on the term at index {index} "
                        "but not on the prefix before it",
                    )
                break
