"""The paper's voting rules, driven synchronously through node.handle."""

from __future__ import annotations

import random

from raft.node import CANDIDATE, FOLLOWER, RaftNode, Safeguards
from raft.storage import Entry, MemoryStorage, PersistentState


def make_node(
    *, term: int = 0, log: list[Entry] | None = None, safeguards: Safeguards | None = None
) -> RaftNode:
    disk = MemoryStorage()
    disk.save(PersistentState(term=term, voted_for=None, log=log or []))
    return RaftNode(
        "n1", ["n2", "n3"], disk, rng=random.Random(7), safeguards=safeguards
    )


def ask(node: RaftNode, *, term: int, candidate: str = "n2",
        last_index: int = 0, last_term: int = 0) -> dict[str, object]:
    return node.handle({
        "op": "request_vote", "term": term, "candidate": candidate,
        "last_log_index": last_index, "last_log_term": last_term,
    })


def test_grants_an_up_to_date_candidate() -> None:
    node = make_node()
    reply = ask(node, term=1)
    assert reply == {"term": 1, "granted": True}
    assert node.term == 1


def test_refuses_a_second_candidate_in_the_same_term() -> None:
    node = make_node()
    assert ask(node, term=1, candidate="n2")["granted"] is True
    assert ask(node, term=1, candidate="n3")["granted"] is False


def test_repeats_its_grant_to_the_same_candidate() -> None:
    node = make_node()
    assert ask(node, term=1, candidate="n2")["granted"] is True
    assert ask(node, term=1, candidate="n2")["granted"] is True


def test_refuses_a_stale_term() -> None:
    node = make_node(term=5)
    reply = ask(node, term=4, last_index=9, last_term=4)
    assert reply == {"term": 5, "granted": False}


def test_refuses_a_candidate_with_a_shorter_log() -> None:
    node = make_node(log=[Entry(1, "a"), Entry(1, "b")])
    assert ask(node, term=2, last_index=1, last_term=1)["granted"] is False


def test_refuses_a_candidate_with_an_older_last_term() -> None:
    node = make_node(term=2, log=[Entry(2, "a")])
    assert ask(node, term=3, last_index=5, last_term=1)["granted"] is False


def test_a_higher_term_resets_an_earlier_vote() -> None:
    node = make_node()
    assert ask(node, term=1, candidate="n2")["granted"] is True
    assert ask(node, term=2, candidate="n3")["granted"] is True
    assert node.term == 2


def test_a_higher_term_demotes_without_gating_the_vote() -> None:
    node = make_node()
    node.role = CANDIDATE
    ask(node, term=9)
    assert node.role == FOLLOWER


def test_votes_survive_a_reload() -> None:
    disk = MemoryStorage()
    node = RaftNode("n1", ["n2", "n3"], disk, rng=random.Random(7))
    node.handle({"op": "request_vote", "term": 1, "candidate": "n2",
                 "last_log_index": 0, "last_log_term": 0})
    reborn = RaftNode("n1", ["n2", "n3"], disk, rng=random.Random(7))
    assert reborn.term == 1
    assert ask(reborn, term=1, candidate="n3")["granted"] is False


def test_without_the_vote_ledger_it_grants_everyone() -> None:
    node = make_node(safeguards=Safeguards(one_vote_per_term=False))
    assert ask(node, term=1, candidate="n2")["granted"] is True
    assert ask(node, term=1, candidate="n3")["granted"] is True


def test_without_the_freshness_check_a_stale_log_wins_votes() -> None:
    node = make_node(log=[Entry(1, "a")], safeguards=Safeguards(check_log_up_to_date=False))
    assert ask(node, term=2, last_index=0, last_term=0)["granted"] is True


def test_without_persistence_a_reload_forgets_the_vote() -> None:
    disk = MemoryStorage()
    relaxed = Safeguards(persist_before_reply=False)
    node = RaftNode("n1", ["n2", "n3"], disk, rng=random.Random(7), safeguards=relaxed)
    node.handle({"op": "request_vote", "term": 1, "candidate": "n2",
                 "last_log_index": 0, "last_log_term": 0})
    reborn = RaftNode("n1", ["n2", "n3"], disk, rng=random.Random(7), safeguards=relaxed)
    assert ask(reborn, term=1, candidate="n3")["granted"] is True
