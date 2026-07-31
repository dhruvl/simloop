"""One Raft peer: elections, log replication, and an applied state machine.

Plain asyncio on streams — nothing here imports simloop. The paper's rules
live in small named methods, and every safety-critical rule sits behind a
``Safeguards`` flag so the test suite can switch one off and watch the seed
explorer find the violation it causes.

Indices are 1-based, as in the paper: raft index ``i`` is ``log[i - 1]``.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from raft import wire
from raft.storage import Entry, Storage

PORT = 4400

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"

# One observable fact, appended in order: ("leader", name, term, log_snapshot)
# or ("apply", name, index, term, command). The safety checks read these.
Event = tuple[Any, ...]


@dataclass(frozen=True)
class Safeguards:
    """The load-bearing rules, individually removable to prove each matters."""

    reject_stale_term: bool = True      # drop RPCs from superseded terms
    one_vote_per_term: bool = True      # never grant two candidates one term
    check_log_up_to_date: bool = True   # §5.4.1: voters gate on log freshness
    persist_before_reply: bool = True   # durability before acknowledgement
    commit_own_term_only: bool = True   # §5.4.2: count replicas only for own-term entries


class RaftNode:
    def __init__(
        self,
        name: str,
        peers: list[str],
        storage: Storage,
        *,
        rng: random.Random,
        port: int = PORT,
        heartbeat_s: float = 0.15,
        election_timeout_s: tuple[float, float] = (0.3, 0.6),
        rpc_timeout_s: float = 0.25,
        safeguards: Safeguards | None = None,
        events: list[Event] | None = None,
    ) -> None:
        self._name = name
        self._peers = list(peers)
        self._storage = storage
        self._rng = rng
        self._port = port
        self._heartbeat_s = heartbeat_s
        self._election_timeout_s = election_timeout_s
        self._rpc_timeout_s = rpc_timeout_s
        self._safeguards = safeguards if safeguards is not None else Safeguards()
        self._events = events
        self._state = storage.load()
        self._quorum = (len(peers) + 1) // 2 + 1
        self.role = FOLLOWER
        self.commit_index = 0
        self.applied: list[Entry] = []
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._reset = asyncio.Event()

    @property
    def name(self) -> str:
        return self._name

    @property
    def term(self) -> int:
        return self._state.term

    @property
    def log(self) -> tuple[Entry, ...]:
        return tuple(self._state.log)

    # ------------------------------------------------------------------
    # RPC handling: synchronous, so a request is atomic between awaits
    # ------------------------------------------------------------------

    def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        op = message["op"]
        if op == "request_vote":
            return self._handle_request_vote(message)
        if op == "append_entries":
            return self._handle_append_entries(message)
        if op == "propose":
            return self._handle_propose(message)
        return {"error": f"unknown op {op!r}"}

    def _handle_request_vote(self, m: dict[str, Any]) -> dict[str, Any]:
        if m["term"] > self._state.term:
            self._become_follower(m["term"])
        refused = {"term": self._state.term, "granted": False}
        if self._safeguards.reject_stale_term and m["term"] < self._state.term:
            return refused
        if self._safeguards.one_vote_per_term and self._state.voted_for not in (
            None,
            m["candidate"],
        ):
            return refused
        ours = (self._last_log_term(), len(self._state.log))
        theirs = (m["last_log_term"], m["last_log_index"])
        if self._safeguards.check_log_up_to_date and theirs < ours:
            return refused
        self._state.voted_for = m["candidate"]
        self._persist()
        self._reset.set()  # granting a vote defers our own candidacy
        return {"term": self._state.term, "granted": True}

    def _handle_append_entries(self, m: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError  # next slice

    def _handle_propose(self, m: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError  # next slice

    # ------------------------------------------------------------------
    # Shared transitions and bookkeeping
    # ------------------------------------------------------------------

    def _become_follower(self, term: int) -> None:
        self._state.term = term
        self._state.voted_for = None
        self._persist()
        self.role = FOLLOWER

    def _persist(self) -> None:
        if self._safeguards.persist_before_reply:
            self._storage.save(self._state)

    def _last_log_term(self) -> int:
        return self._state.log[-1].term if self._state.log else 0

    def _record(self, *event: Any) -> None:
        if self._events is not None:
            self._events.append(event)
