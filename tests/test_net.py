"""Host registry, task pinning, and the simulated packet network."""

from __future__ import annotations

import asyncio

import pytest

from simloop import Host, SimLoop, SimNetwork


def test_loop_exposes_a_network() -> None:
    loop = SimLoop(seed=0)
    try:
        assert isinstance(loop.net, SimNetwork)
    finally:
        loop.close()


def test_host_registration_and_validation() -> None:
    loop = SimLoop(seed=0)
    try:
        node = loop.net.host("node1")
        assert isinstance(node, Host)
        assert node.name == "node1"
        with pytest.raises(ValueError, match="already"):
            loop.net.host("node1")
        with pytest.raises(ValueError, match="already"):
            loop.net.host("driver")  # implicit driver host is pre-registered
        for bad in ("", "a|b", "a>b", "a\nb"):
            with pytest.raises(ValueError):
                loop.net.host(bad)
    finally:
        loop.close()


def test_tasks_are_pinned_to_their_host() -> None:
    loop = SimLoop(seed=0)
    seen: list[str] = []

    async def whoami() -> None:
        from simloop._net import _current_host

        seen.append(_current_host.get())

    async def parent() -> None:
        # A child task created inside a pinned task inherits the pin.
        await asyncio.create_task(whoami())

    async def main() -> None:
        node = loop.net.host("node1")
        await node.create_task(parent())
        await asyncio.create_task(whoami())  # unpinned: belongs to the driver

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert seen == ["node1", "driver"]


def test_task_registry_tracks_creation_and_completion() -> None:
    loop = SimLoop(seed=0)

    async def nap() -> None:
        await asyncio.sleep(0.01)

    async def main() -> None:
        node = loop.net.host("node1")
        task = node.create_task(nap())
        assert task in loop.net._tasks["node1"]
        await task
        # The clock only advances once the ready queue is drained, so after
        # this sleep the removal callback has certainly run.
        await asyncio.sleep(0.01)
        assert loop.net._tasks["node1"] == []

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
