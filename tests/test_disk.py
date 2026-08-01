"""Per-host storage that survives the machine crashing."""

from __future__ import annotations

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
