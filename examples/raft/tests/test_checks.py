"""The safety checkers themselves, on synthetic histories."""

from __future__ import annotations

import pytest

from raft.storage import Entry

from checks import InvariantViolation, check_invariants


def test_two_leaders_in_one_term_is_flagged() -> None:
    events = [("leader", "n1", 3, ()), ("leader", "n2", 3, ())]
    with pytest.raises(InvariantViolation) as caught:
        check_invariants({}, events)
    assert caught.value.invariant == "election-safety"


def test_reelection_across_terms_is_fine() -> None:
    check_invariants({}, [("leader", "n1", 3, ()), ("leader", "n2", 4, ())])


def test_conflicting_applies_at_one_index_are_flagged() -> None:
    events = [("apply", "n1", 1, 1, "a"), ("apply", "n2", 1, 2, "b")]
    with pytest.raises(InvariantViolation) as caught:
        check_invariants({}, events)
    assert caught.value.invariant == "state-machine-safety"


def test_matching_applies_are_fine() -> None:
    check_invariants({}, [("apply", "n1", 1, 1, "a"), ("apply", "n2", 1, 1, "a")])


def test_a_leader_missing_a_committed_entry_is_flagged() -> None:
    events = [("apply", "n1", 1, 1, "a"), ("leader", "n2", 2, ())]
    with pytest.raises(InvariantViolation) as caught:
        check_invariants({}, events)
    assert caught.value.invariant == "leader-completeness"


def test_a_leader_carrying_the_committed_prefix_is_fine() -> None:
    events = [
        ("apply", "n1", 1, 1, "a"),
        ("leader", "n2", 2, (Entry(1, "a"), Entry(1, "b"))),
    ]
    check_invariants({}, events)


def test_a_leader_event_before_any_apply_is_fine() -> None:
    check_invariants({}, [("leader", "n1", 1, ()), ("apply", "n2", 1, 1, "a")])


def test_a_stale_term_leader_after_a_commit_is_not_judged() -> None:
    events = [
        ("leader", "n1", 3, (Entry(1, "a"),)),
        ("apply", "n1", 1, 1, "a"),
        ("leader", "n2", 2, ()),  # straggler election below the commit floor
    ]
    check_invariants({}, events)


def test_a_higher_term_leader_missing_the_entry_still_fires() -> None:
    events = [
        ("leader", "n1", 3, (Entry(1, "a"),)),
        ("apply", "n1", 1, 1, "a"),
        ("leader", "n2", 4, ()),
    ]
    with pytest.raises(InvariantViolation) as caught:
        check_invariants({}, events)
    assert caught.value.invariant == "leader-completeness"


def test_shared_terms_with_different_prefixes_are_flagged() -> None:
    logs: dict[str, tuple[Entry, ...]] = {
        "n1": (Entry(1, "a"), Entry(2, "c")),
        "n2": (Entry(1, "b"), Entry(2, "c")),
    }
    with pytest.raises(InvariantViolation) as caught:
        check_invariants(logs, [])
    assert caught.value.invariant == "log-matching"


def test_diverged_tails_with_distinct_terms_are_fine() -> None:
    logs: dict[str, tuple[Entry, ...]] = {
        "n1": (Entry(1, "a"), Entry(2, "x")),
        "n2": (Entry(1, "a"), Entry(3, "y")),
    }
    check_invariants(logs, [])


def test_a_clean_history_passes() -> None:
    logs: dict[str, tuple[Entry, ...]] = {
        "n1": (Entry(1, "a"),),
        "n2": (Entry(1, "a"),),
    }
    events = [
        ("leader", "n1", 1, ()),
        ("apply", "n1", 1, 1, "a"),
        ("apply", "n2", 1, 1, "a"),
    ]
    check_invariants(logs, events)
