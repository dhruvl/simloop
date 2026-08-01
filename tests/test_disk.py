"""Per-host storage that survives the machine crashing."""

from __future__ import annotations

import asyncio

import pytest

from simloop import SimLoop


def _network() -> SimLoop:
    loop = SimLoop(seed=0)
    loop.net.host("server")
    loop.net.host("client")
    return loop


def test_disk_survives_crash_and_restart() -> None:
    loop = _network()
    loop.net.host("server").disk["term"] = 7
    loop.net.crash("server")
    loop.net.restart("server")
    assert loop.net.host("server").disk["term"] == 7
    loop.close()


def test_disks_are_per_host() -> None:
    loop = _network()
    loop.net.host("server").disk["k"] = "s"
    loop.net.host("client").disk["k"] = "c"
    assert loop.net.host("server").disk["k"] == "s"
    assert loop.net.host("client").disk["k"] == "c"
    loop.close()


def test_disk_is_a_real_mapping() -> None:
    loop = _network()
    disk = loop.net.host("server").disk
    disk["a"] = 1
    disk["b"] = 2
    assert len(disk) == 2
    assert sorted(disk) == ["a", "b"]
    del disk["a"]
    with pytest.raises(KeyError):
        disk["a"]
    disk.clear()
    assert len(disk) == 0
    loop.close()


def test_the_driver_has_a_disk_too() -> None:
    loop = _network()
    loop.net.host("driver").disk["x"] = 1
    assert loop.net.host("driver").disk["x"] == 1
    loop.close()


def test_sync_is_a_no_op_on_an_unbuffered_disk() -> None:
    loop = _network()
    disk = loop.net.host("server").disk
    disk["term"] = 7
    disk.sync()  # application code calls it whether or not the disk buffers
    loop.net.crash("server")
    assert loop.net.host("server").disk["term"] == 7
    loop.close()


def test_a_buffered_write_is_not_durable_until_sync() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["term"] = 7
    loop.net.crash("server")
    loop.net.restart("server")
    assert "term" not in loop.net.host("server").disk
    loop.close()


def test_sync_makes_a_buffered_write_survive_the_crash() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["term"] = 7
    disk.sync()
    disk["vote"] = "n2"  # written after the flush, so this one is lost
    loop.net.crash("server")
    loop.net.restart("server")
    assert dict(loop.net.host("server").disk) == {"term": 7}
    loop.close()


def test_a_buffered_disk_reads_its_own_writes() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["a"] = 1
    disk.sync()
    disk["a"] = 2
    disk["b"] = 3
    assert disk["a"] == 2
    assert disk["b"] == 3
    assert len(disk) == 2
    loop.close()


def test_a_pending_delete_hides_a_durable_key() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["a"] = 1
    disk.sync()
    del disk["a"]
    assert "a" not in disk
    assert len(disk) == 0
    with pytest.raises(KeyError):
        disk["a"]
    with pytest.raises(KeyError):
        del disk["a"]
    loop.net.crash("server")
    loop.net.restart("server")
    assert dict(loop.net.host("server").disk) == {"a": 1}  # the delete never landed
    loop.close()


def test_the_merged_view_iterates_durable_order_then_write_order() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["a"] = 1
    disk["b"] = 2
    disk.sync()
    disk["z"] = 26
    disk["a"] = 10  # an overwrite keeps the key where the durable state has it
    disk["c"] = 3
    del disk["b"]
    assert list(disk) == ["a", "z", "c"]
    disk.sync()
    assert list(disk) == ["a", "z", "c"]  # the flush leaves the order it showed
    loop.close()


def test_a_crash_with_nothing_pending_keeps_everything() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True, torn=True)
    disk = loop.net.host("server").disk
    disk["a"] = 1
    disk.sync()
    loop.net.crash("server")
    loop.net.restart("server")
    assert dict(loop.net.host("server").disk) == {"a": 1}
    loop.close()


def _torn_prefix(seed: int, *, torn: bool = True, writes: int = 20) -> list[str]:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.set_disk("server", buffered=True, torn=torn)
    disk = loop.net.host("server").disk
    for i in range(writes):
        disk[f"k{i}"] = i
    loop.net.crash("server")
    loop.net.restart("server")
    survivors = list(loop.net.host("server").disk)
    loop.close()
    return survivors


def test_a_torn_crash_keeps_a_prefix_of_the_journal() -> None:
    survivors = _torn_prefix(0)
    assert survivors == [f"k{i}" for i in range(len(survivors))]
    assert 0 <= len(survivors) <= 20


def test_the_same_seed_tears_at_the_same_place() -> None:
    assert _torn_prefix(3) == _torn_prefix(3) == _torn_prefix(3)


def test_different_seeds_tear_at_different_places() -> None:
    lengths = {len(_torn_prefix(seed)) for seed in range(12)}
    assert len(lengths) > 1


def test_without_torn_a_crash_keeps_nothing_pending() -> None:
    assert _torn_prefix(3, torn=False) == []


def test_torn_writes_need_a_buffer_to_tear() -> None:
    loop = _network()
    with pytest.raises(ValueError):
        loop.net.set_disk("server", torn=True)
    with pytest.raises(OSError):
        loop.net.set_disk("ghost", buffered=True)
    loop.close()


def test_reconfiguring_flushes_what_was_pending() -> None:
    loop = _network()
    loop.net.set_disk("server", buffered=True)
    disk = loop.net.host("server").disk
    disk["a"] = 1
    loop.net.set_disk("server", buffered=False)
    assert not disk.buffered
    loop.net.crash("server")
    loop.net.restart("server")
    assert dict(loop.net.host("server").disk) == {"a": 1}
    loop.close()


def test_a_configured_disk_keeps_what_it_already_held() -> None:
    loop = _network()
    loop.net.host("server").disk["a"] = 1
    loop.net.set_disk("server", buffered=True, torn=True)
    disk = loop.net.host("server").disk
    assert disk["a"] == 1
    assert (disk.buffered, disk.torn) == (True, True)
    loop.close()


def _traced(configure: bool) -> str:
    loop = SimLoop(seed=1)
    loop.net.host("server")
    if configure:
        loop.net.set_disk("server", buffered=True)

    async def main() -> None:
        disk = loop.net.host("server").disk
        for i in range(5):
            disk[f"k{i}"] = i
            await asyncio.sleep(0.1)
        disk.sync()

    try:
        loop.run_until_complete(loop.net.host("server").create_task(main()))
        return loop.trace_hash()
    finally:
        loop.close()


def test_buffering_a_disk_changes_no_scheduling_decision() -> None:
    # Storage is not a scheduling event: the buffer changes when a value
    # becomes durable and nothing about what ran when.
    assert _traced(False) == _traced(True)


def test_a_buffered_crash_leaves_the_network_draws_alone() -> None:
    def run(buffered: bool) -> tuple[str, list[str]]:
        loop = SimLoop(seed=5)
        loop.net.host("server")
        loop.net.host("client")
        loop.net.set_defaults(latency=(0.01, 0.05), drop=0.2, duplicate=0.2)
        if buffered:
            loop.net.set_disk("server", buffered=True, torn=True)

        async def main() -> None:
            class Echo(asyncio.DatagramProtocol):
                def datagram_received(self, data: bytes, addr: object) -> None:
                    seen.append(data)

            seen: list[bytes] = []
            await loop.net.host("server").create_task(
                loop.create_datagram_endpoint(Echo, local_addr=("server", 9000))
            )
            transport, _ = await loop.net.host("client").create_task(
                loop.create_datagram_endpoint(
                    asyncio.DatagramProtocol,
                    local_addr=("client", 9001),
                    remote_addr=("server", 9000),
                )
            )
            disk = loop.net.host("server").disk
            for i in range(10):
                disk[f"k{i}"] = i
                transport.sendto(f"{i}".encode())
                await asyncio.sleep(0.1)
            loop.net.crash("server")
            await asyncio.sleep(0.5)
            arrived.extend(d.decode() for d in seen)

        arrived: list[str] = []
        try:
            loop.run_until_complete(main())
            return loop.trace_hash(), arrived
        finally:
            loop.close()

    # The torn prefix draws from the disk's own seed-derived stream, so which
    # datagrams the network dropped is the same either way.
    assert run(False) == run(True)
