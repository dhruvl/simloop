"""AppendEntries: consistency check, truncation, commit, apply."""

from __future__ import annotations

import random

from raft.node import LEADER, Event, RaftNode, Safeguards
from raft.storage import Entry, MemoryStorage, PersistentState


def make_node(
    *,
    term: int = 1,
    log: list[Entry] | None = None,
    safeguards: Safeguards | None = None,
    events: list[Event] | None = None,
) -> RaftNode:
    disk = MemoryStorage()
    disk.save(PersistentState(term=term, voted_for=None, log=log or []))
    return RaftNode(
        "n1", ["n2", "n3"], disk, rng=random.Random(7),
        safeguards=safeguards, events=events,
    )


def append(node: RaftNode, *, term: int = 1, prev_index: int = 0, prev_term: int = 0,
           entries: list[list[object]] | None = None, commit: int = 0) -> dict[str, object]:
    return node.handle({
        "op": "append_entries", "term": term, "leader": "n2",
        "prev_log_index": prev_index, "prev_log_term": prev_term,
        "entries": entries or [], "leader_commit": commit,
    })


def test_acks_a_heartbeat_from_a_live_leader() -> None:
    node = make_node()
    assert append(node) == {"term": 1, "ok": True}


def test_refuses_a_stale_leader() -> None:
    node = make_node(term=3)
    assert append(node, term=2) == {"term": 3, "ok": False}


def test_refuses_when_the_previous_entry_is_missing() -> None:
    node = make_node()
    assert append(node, prev_index=2, prev_term=1)["ok"] is False


def test_refuses_when_the_previous_terms_disagree() -> None:
    node = make_node(log=[Entry(1, "a")])
    assert append(node, prev_index=1, prev_term=2)["ok"] is False


def test_appends_new_entries() -> None:
    node = make_node()
    append(node, entries=[[1, "a"], [1, "b"]])
    assert node.log == (Entry(1, "a"), Entry(1, "b"))


def test_replays_are_idempotent() -> None:
    node = make_node(log=[Entry(1, "a"), Entry(1, "b")])
    append(node, entries=[[1, "a"], [1, "b"]])
    assert node.log == (Entry(1, "a"), Entry(1, "b"))


def test_truncates_a_conflicting_suffix() -> None:
    node = make_node(term=2, log=[Entry(1, "a"), Entry(1, "x"), Entry(1, "y")])
    append(node, term=2, prev_index=1, prev_term=1, entries=[[2, "b"]])
    assert node.log == (Entry(1, "a"), Entry(2, "b"))


def test_commits_and_applies_up_to_the_leaders_mark() -> None:
    events: list[Event] = []
    node = make_node(events=events)
    append(node, entries=[[1, "a"], [1, "b"]], commit=1)
    assert node.commit_index == 1
    assert node.applied == [Entry(1, "a")]
    assert events == [("apply", "n1", 1, 1, "a", 1)]


def test_a_noop_advances_commit_but_not_the_state_machine() -> None:
    events: list[Event] = []
    node = make_node(events=events)
    append(node, entries=[[1, ""], [1, "x"]], commit=2)
    assert node.commit_index == 2
    assert node.applied == [Entry(1, "x")]
    assert events == [("apply", "n1", 2, 1, "x", 1)]


def test_never_commits_past_what_it_verified() -> None:
    node = make_node()
    append(node, entries=[[1, "a"]], commit=9)
    assert node.commit_index == 1


def test_commit_never_regresses_on_a_backed_off_heartbeat() -> None:
    node = make_node()
    append(node, entries=[[1, "a"], [1, "b"]], commit=2)
    assert node.commit_index == 2
    append(node, prev_index=1, prev_term=1, commit=9)
    assert node.commit_index == 2


def test_a_higher_term_message_demotes_and_updates() -> None:
    node = make_node()
    node.role = LEADER
    reply = append(node, term=5)
    assert reply == {"term": 5, "ok": True}
    assert node.role == "follower"


def test_a_leaders_message_demotes_a_candidate_of_the_same_term() -> None:
    node = make_node()
    node.role = "candidate"
    append(node)
    assert node.role == "follower"


def test_without_the_term_check_a_stale_leader_rewrites_history() -> None:
    node = make_node(
        term=3, log=[Entry(3, "new")], safeguards=Safeguards(reject_stale_term=False)
    )
    reply = append(node, term=1, entries=[[1, "old"]])
    assert reply["ok"] is True
    assert node.log == (Entry(1, "old"),)


def test_propose_is_refused_off_the_leader() -> None:
    node = make_node()
    assert node.handle({"op": "propose", "command": "x"}) == {"ok": False}


def test_propose_appends_on_the_leader() -> None:
    node = make_node(term=2)
    node.role = LEADER
    reply = node.handle({"op": "propose", "command": "x"})
    assert reply == {"ok": True, "index": 1, "term": 2}
    assert node.log == (Entry(2, "x"),)


def test_propose_refuses_an_empty_command() -> None:
    node = make_node(term=2)
    node.role = LEADER
    assert node.handle({"op": "propose", "command": ""}) == {"ok": False}


def test_a_new_leader_opens_its_term_with_a_noop() -> None:
    node = make_node(term=3, log=[Entry(1, "a")])
    node._become_leader()
    assert node.log == (Entry(1, "a"), Entry(3, ""))
    assert node._next_index == {"n2": 2, "n3": 2}
    assert node._match_index == {"n2": 0, "n3": 0}


def test_the_noop_stays_off_when_disabled() -> None:
    node = make_node(
        term=3, log=[Entry(1, "a")], safeguards=Safeguards(leader_noop=False)
    )
    node._become_leader()
    assert node.log == (Entry(1, "a"),)
