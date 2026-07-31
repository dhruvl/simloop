"""One Raft peer: elections, log replication, and an applied state machine.

Plain asyncio on streams — nothing here imports simloop. The paper's rules
live in small named methods, and every safety-critical rule sits behind a
``Safeguards`` flag so the test suite can switch one off and watch the seed
explorer find the violation it causes.

Indices are 1-based, as in the paper: raft index ``i`` is ``log[i - 1]``.
The empty command is reserved: a leader opens each term with a no-op entry the
state machine never sees, and propose refuses it.
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
    # §5.4.2: count replicas only for own-term entries. With leader_noop on,
    # every reachable quorum index already carries the leader's term, so this
    # gate only shows its teeth when the no-op is off too.
    commit_own_term_only: bool = True
    leader_noop: bool = True            # §8: open each term with a no-op so a quiet term can still commit


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
        election_timeout_s: tuple[float, float] = (0.45, 0.9),
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
        self._applied_index = 0
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._reset = asyncio.Event()
        self._stopped = False

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
    # The run loop: serve RPCs while cycling follower -> candidate -> leader
    # ------------------------------------------------------------------

    async def run(self) -> None:
        handlers: set[asyncio.Task[None]] = set()

        async def connection(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            task = asyncio.current_task()
            assert task is not None
            handlers.add(task)
            task.add_done_callback(handlers.discard)
            await self._connection(reader, writer)

        server = await asyncio.start_server(connection, "0.0.0.0", self._port)
        try:
            while True:
                if self.role == FOLLOWER:
                    await self._follow()
                elif self.role == CANDIDATE:
                    await self._campaign()
                else:
                    await self._lead()
        finally:
            # A handler parked mid-request outlives this incarnation otherwise,
            # and it still holds the storage the next one boots from: it would
            # answer for a node that no longer exists, rolling the live term or
            # vote backwards behind its back. The flag goes first because a
            # handler started in the turn before it joined ``handlers`` is not
            # in the sweep below; it checks the flag instead.
            self._stopped = True
            server.close()
            for task in list(handlers):
                task.cancel()

    async def _connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            message = await wire.read_message(reader)
            if self._stopped:
                return  # a dead incarnation must not touch shared storage
            wire.write_message(writer, self.handle(message))
            await writer.drain()
        except (asyncio.IncompleteReadError, OSError, wire.FrameError):
            # IncompleteReadError is named on its own because it is not an
            # OSError. OSError here means a torn socket -- which also swallows
            # one raised out of handle(), acceptable only while storage is in
            # memory; a file-backed Storage needs its own except clause.
            pass
        finally:
            writer.close()

    async def _follow(self) -> None:
        self._reset.clear()
        try:
            async with asyncio.timeout(self._rng.uniform(*self._election_timeout_s)):
                await self._reset.wait()
        except TimeoutError:
            self.role = CANDIDATE

    async def _campaign(self) -> None:
        self._state.term += 1
        self._state.voted_for = self._name
        self._persist()
        term = self._state.term
        granted = 1
        settled = asyncio.Event()  # the election is decided, one way or the other

        async def solicit(peer: str) -> None:
            nonlocal granted
            reply = await wire.call(
                peer,
                self._port,
                {
                    "op": "request_vote",
                    "term": term,
                    "candidate": self._name,
                    "last_log_index": len(self._state.log),
                    "last_log_term": self._last_log_term(),
                },
                timeout_s=self._rpc_timeout_s,
            )
            if reply is None:
                return
            if reply["term"] > self._state.term:
                # Ahead of the staleness guard below: a straggler reply from an
                # abandoned candidacy still carries news we have to act on.
                self._become_follower(reply["term"])
                settled.set()
                return
            if self.role != CANDIDATE or self._state.term != term:
                return
            if reply["granted"]:
                granted += 1
                if granted >= self._quorum:
                    settled.set()

        solicitors = [asyncio.create_task(solicit(peer)) for peer in self._peers]
        try:
            async with asyncio.timeout(self._rng.uniform(*self._election_timeout_s)):
                await settled.wait()
        except TimeoutError:
            return  # split vote or a lost election: run() re-reads the role
        finally:
            for task in solicitors:
                task.cancel()
            await asyncio.gather(*solicitors, return_exceptions=True)
        if self.role == CANDIDATE and self._state.term == term:
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = LEADER
        self._next_index = {peer: len(self._state.log) + 1 for peer in self._peers}
        self._match_index = {peer: 0 for peer in self._peers}
        # A term with no client traffic must still be able to commit: the
        # own-term rule only counts replicas for this term, so open the term
        # with a no-op entry the state machine will never see. The indices
        # above are deliberately computed before this append, so the first
        # push to each peer carries the no-op against a prev the peer has.
        if self._safeguards.leader_noop:
            self._state.log.append(Entry(self._state.term, ""))
            self._persist()
        self._record("leader", self._name, self._state.term, tuple(self._state.log))

    async def _lead(self) -> None:
        term = self._state.term
        pushers = [
            asyncio.create_task(self._push(peer, term)) for peer in self._peers
        ]
        try:
            while self.role == LEADER and self._state.term == term:
                await asyncio.sleep(self._heartbeat_s)
        finally:
            for task in pushers:
                task.cancel()
            await asyncio.gather(*pushers, return_exceptions=True)

    async def _push(self, peer: str, term: int) -> None:
        loop = asyncio.get_running_loop()
        while True:
            sent_at = loop.time()
            prev = self._next_index[peer] - 1
            entries = self._state.log[prev:]
            reply = await wire.call(
                peer,
                self._port,
                {
                    "op": "append_entries",
                    "term": term,
                    "leader": self._name,
                    "prev_log_index": prev,
                    "prev_log_term": self._state.log[prev - 1].term if prev > 0 else 0,
                    "entries": [[entry.term, entry.command] for entry in entries],
                    "leader_commit": self.commit_index,
                },
                timeout_s=self._rpc_timeout_s,
            )
            if reply is not None and reply["term"] > self._state.term:
                # Ahead of the staleness guard below: a straggler reply from a
                # deposed leadership still carries news we have to act on.
                self._become_follower(reply["term"])
                return
            if self.role != LEADER or self._state.term != term:
                return
            if reply is not None:
                if reply["ok"]:
                    self._match_index[peer] = prev + len(entries)
                    self._next_index[peer] = prev + len(entries) + 1
                    self._advance_commit(term)
                else:
                    self._next_index[peer] = max(1, self._next_index[peer] - 1)
                    continue  # retry straight away, one entry earlier
            # The pause runs from the send, not from the reply: pausing a whole
            # interval *after* a round trip would make the real heartbeat period
            # interval + round trip, which drifts into the follower's election
            # timeout as soon as the links are slow.
            await asyncio.sleep(max(0.0, sent_at + self._heartbeat_s - loop.time()))

    def _advance_commit(self, term: int) -> None:
        for index in range(len(self._state.log), self.commit_index, -1):
            replicas = 1 + sum(
                1 for match in self._match_index.values() if match >= index
            )
            if replicas < self._quorum:
                continue
            if (
                self._safeguards.commit_own_term_only
                and self._state.log[index - 1].term != term
            ):
                # Log terms never decrease, so once the scan walks back past our
                # own term nothing below it can qualify either.
                return
            self.commit_index = index
            self._apply()
            return

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
        if m["term"] > self._state.term:
            self._become_follower(m["term"])
        if self._safeguards.reject_stale_term and m["term"] < self._state.term:
            return {"term": self._state.term, "ok": False}
        if self.role == CANDIDATE:
            self.role = FOLLOWER  # a live leader for this term beat us to it
        self._reset.set()
        log = self._state.log
        prev = m["prev_log_index"]
        if prev > len(log) or (prev > 0 and log[prev - 1].term != m["prev_log_term"]):
            return {"term": self._state.term, "ok": False}
        for offset, (entry_term, command) in enumerate(m["entries"]):
            index = prev + 1 + offset
            if index <= len(log) and log[index - 1].term != entry_term:
                del log[index - 1 :]
            if index > len(log):
                log.append(Entry(entry_term, command))
        self._persist()
        if m["leader_commit"] > self.commit_index:
            # max(): a backed-off heartbeat carries a short verified prefix;
            # the commit mark never moves backwards for it.
            self.commit_index = max(
                self.commit_index, min(m["leader_commit"], prev + len(m["entries"]))
            )
            self._apply()
        return {"term": self._state.term, "ok": True}

    def _handle_propose(self, m: dict[str, Any]) -> dict[str, Any]:
        if self.role != LEADER:
            return {"ok": False}
        if not m["command"]:
            return {"ok": False}  # the empty command is the no-op sentinel
        self._state.log.append(Entry(self._state.term, m["command"]))
        self._persist()
        return {"ok": True, "index": len(self._state.log), "term": self._state.term}

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

    def _apply(self) -> None:
        # The min() only matters when a disabled safeguard let the log shrink
        # below the commit mark: apply what exists, and let the safety checks
        # name the violation instead of an IndexError naming it first.
        # The cursor is a raft index, not a count of applied entries: a no-op
        # advances it without reaching the state machine, so the two diverge.
        while self._applied_index < min(self.commit_index, len(self._state.log)):
            entry = self._state.log[self._applied_index]
            self._applied_index += 1
            if entry.command:
                self._record(
                    "apply", self._name, self._applied_index, entry.term, entry.command
                )
                self.applied.append(entry)
