"""What survives a restart: the storage contract."""

from __future__ import annotations

from simloop import SimLoop

from raft.storage import DiskStorage, Entry, MemoryStorage, PersistentState


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


def _powered(seed: int = 0, *, torn: bool = True) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("n1")
    loop.net.set_disk("n1", buffered=True, torn=torn)
    return loop


def test_disk_storage_round_trips_through_a_host_disk() -> None:
    loop = _powered()
    storage = DiskStorage(loop.net.host("n1").disk)
    storage.save(PersistentState(term=3, voted_for="n2", log=[Entry(1, "a")]))
    storage.sync()
    state = storage.load()
    assert (state.term, state.voted_for, state.log) == (3, "n2", [Entry(1, "a")])
    loop.close()


def test_a_fresh_host_disk_loads_empty_state() -> None:
    loop = _powered()
    state = DiskStorage(loop.net.host("n1").disk).load()
    assert (state.term, state.voted_for, state.log) == (0, None, [])
    loop.close()


def test_the_power_cut_takes_what_was_never_synced() -> None:
    loop = _powered(torn=False)
    storage = DiskStorage(loop.net.host("n1").disk)
    storage.save(PersistentState(term=1, voted_for="n1", log=[]))
    storage.sync()
    storage.save(PersistentState(term=2, voted_for="n3", log=[Entry(2, "a")]))
    loop.net.crash("n1")
    loop.net.restart("n1")
    state = DiskStorage(loop.net.host("n1").disk).load()
    assert (state.term, state.voted_for, state.log) == (1, "n1", [])
    loop.close()


def test_a_torn_crash_lands_on_a_whole_record() -> None:
    # Each save is one write, so a torn crash can rewind the state to any
    # record the node wrote -- never to half of one, which is why the state
    # goes down as a single value.
    written = [
        PersistentState(term=term, voted_for=f"n{term}", log=[Entry(term, "a")])
        for term in range(1, 9)
    ]
    for seed in range(6):
        loop = _powered(seed)
        storage = DiskStorage(loop.net.host("n1").disk)
        for state in written:
            storage.save(state)
        loop.net.crash("n1")
        loop.net.restart("n1")
        landed = DiskStorage(loop.net.host("n1").disk).load()
        loop.close()
        assert landed in [PersistentState()] + written
