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
        assert loop.net.host("node1") is node  # repeat lookup returns the same host
        assert loop.net.host("driver").name == "driver"  # implicit driver host
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


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[tuple[bytes, tuple[str, int]]] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append((data, addr))


async def _bound_endpoint(
    host: Host, port: int
) -> tuple[asyncio.DatagramTransport, _Collector]:
    loop = asyncio.get_running_loop()

    async def bind() -> tuple[asyncio.DatagramTransport, _Collector]:
        result: tuple[asyncio.DatagramTransport, _Collector] = (
            await loop.create_datagram_endpoint(
                _Collector, local_addr=("0.0.0.0", port)
            )
        )
        return result

    task: asyncio.Task[tuple[asyncio.DatagramTransport, _Collector]] = host.create_task(
        bind()
    )
    return await task


def _run_datagram_exchange(
    seed: int, *, drop: float = 0.0, duplicate: float = 0.0, count: int = 1
) -> tuple[list[tuple[bytes, tuple[str, int]]], SimLoop]:
    loop = SimLoop(seed=seed)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    loop.net.set_link("beta", "alpha", drop=drop, duplicate=duplicate)

    async def main() -> list[tuple[bytes, tuple[str, int]]]:
        transport_a, collector = await _bound_endpoint(alpha, 7000)
        transport_b, _ = await _bound_endpoint(beta, 7001)

        async def send_all() -> None:
            for number in range(count):
                transport_b.sendto(f"ping{number}".encode(), ("alpha", 7000))

        await beta.create_task(send_all())
        await asyncio.sleep(1.0)
        transport_a.close()
        transport_b.close()
        await asyncio.sleep(0.01)
        return collector.received

    try:
        received = loop.run_until_complete(main())
    finally:
        loop.close()
    return received, loop


def test_datagram_reaches_its_destination_with_source_address() -> None:
    received, _ = _run_datagram_exchange(seed=0)
    assert received == [(b"ping0", ("beta", 7001))]


def test_datagram_drop_probability_one_loses_everything() -> None:
    received, loop = _run_datagram_exchange(seed=0, drop=1.0, count=5)
    assert received == []
    labels = [e.label for e in loop.trace if e.kind == "net"]
    assert labels.count("drop beta>alpha") == 5


def test_datagram_duplicate_probability_one_doubles_everything() -> None:
    received, loop = _run_datagram_exchange(seed=0, duplicate=1.0, count=3)
    assert len(received) == 6
    labels = [e.label for e in loop.trace if e.kind == "net"]
    assert labels.count("dup beta>alpha") == 3


def test_latency_reorders_but_replays_identically() -> None:
    def run(seed: int) -> tuple[list[tuple[bytes, tuple[str, int]]], str]:
        loop = SimLoop(seed=seed)
        alpha = loop.net.host("alpha")
        beta = loop.net.host("beta")
        loop.net.set_defaults(latency=(0.001, 0.1))

        async def main() -> list[tuple[bytes, tuple[str, int]]]:
            transport_a, collector = await _bound_endpoint(alpha, 7000)
            transport_b, _ = await _bound_endpoint(beta, 7001)
            for number in range(20):
                transport_b.sendto(f"m{number:02d}".encode(), ("alpha", 7000))
            await asyncio.sleep(2.0)
            transport_a.close()
            transport_b.close()
            await asyncio.sleep(0.01)
            return collector.received

        try:
            result = loop.run_until_complete(main())
        finally:
            loop.close()
        return result, loop.trace_hash()

    first, first_hash = run(3)
    again, again_hash = run(3)
    assert first == again
    assert first_hash == again_hash
    in_order = sorted(first, key=lambda item: item[0])
    assert first != in_order  # at least one pair actually arrived reordered


def test_unknown_destination_host_raises() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")

    async def main() -> None:
        transport, _ = await _bound_endpoint(alpha, 7000)
        with pytest.raises(OSError, match="unknown host"):
            transport.sendto(b"x", ("ghost", 1))
        transport.close()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_fault_config_is_validated() -> None:
    loop = SimLoop(seed=0)
    try:
        net = loop.net
        net.host("alpha")
        net.host("beta")
        with pytest.raises(ValueError):
            net.set_defaults(drop=1.5)
        with pytest.raises(ValueError):
            net.set_defaults(duplicate=-0.1)
        with pytest.raises(ValueError):
            net.set_defaults(latency=(0.2, 0.1))
        with pytest.raises(ValueError):
            net.set_defaults(latency=(-0.1, 0.1))
        with pytest.raises(OSError, match="unknown host"):
            net.set_link("alpha", "ghost", drop=0.5)
    finally:
        loop.close()


def test_duplicate_datagram_bind_is_rejected() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")

    async def main() -> None:
        transport, _ = await _bound_endpoint(alpha, 7000)
        with pytest.raises(OSError, match="in use"):
            await _bound_endpoint(alpha, 7000)
        transport.close()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
