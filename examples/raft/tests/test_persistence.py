"""What survives a restart: the storage contract."""

from __future__ import annotations

from raft.storage import Entry, MemoryStorage, PersistentState


def test_a_fresh_disk_loads_empty_state() -> None:
    state = MemoryStorage().load()
    assert (state.term, state.voted_for, state.log) == (0, None, [])


def test_saved_state_round_trips() -> None:
    disk = MemoryStorage()
    disk.save(PersistentState(term=3, voted_for="n2", log=[Entry(1, "a")]))
    state = disk.load()
    assert (state.term, state.voted_for, state.log) == (3, "n2", [Entry(1, "a")])


def test_mutating_loaded_state_does_not_write_through() -> None:
    disk = MemoryStorage()
    disk.save(PersistentState(term=1, voted_for=None, log=[Entry(1, "a")]))
    state = disk.load()
    state.log.append(Entry(1, "b"))
    state.term = 9
    assert disk.load().log == [Entry(1, "a")]
    assert disk.load().term == 1


def test_mutating_saved_state_does_not_write_through() -> None:
    disk = MemoryStorage()
    state = PersistentState(term=1, voted_for=None, log=[Entry(1, "a")])
    disk.save(state)
    state.log.append(Entry(1, "b"))
    state.term = 9
    assert disk.load().log == [Entry(1, "a")]
    assert disk.load().term == 1
