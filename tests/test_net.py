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


def test_partition_blackholes_datagrams_and_heal_restores() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")

    async def main() -> list[tuple[bytes, tuple[str, int]]]:
        transport_a, collector = await _bound_endpoint(alpha, 7000)
        transport_b, _ = await _bound_endpoint(beta, 7001)
        loop.net.partition({"alpha"}, {"beta"})
        transport_b.sendto(b"during", ("alpha", 7000))
        await asyncio.sleep(0.5)
        loop.net.heal()
        transport_b.sendto(b"after", ("alpha", 7000))
        await asyncio.sleep(0.5)
        transport_a.close()
        transport_b.close()
        await asyncio.sleep(0.01)
        return collector.received

    try:
        received = loop.run_until_complete(main())
    finally:
        loop.close()
    assert received == [(b"after", ("beta", 7001))]
    labels = [e.label for e in loop.trace if e.kind == "net"]
    assert "drop beta>alpha" in labels


def test_partition_validation() -> None:
    loop = SimLoop(seed=0)
    try:
        net = loop.net
        net.host("alpha")
        net.host("beta")
        with pytest.raises(OSError, match="unknown host"):
            net.partition({"alpha"}, {"ghost"})
        with pytest.raises(ValueError, match="both sides"):
            net.partition({"alpha"}, {"alpha", "beta"})
        with pytest.raises(ValueError, match="non-empty"):
            net.partition(set(), {"alpha"})
    finally:
        loop.close()


def test_driver_is_unaffected_by_partitions_it_is_not_named_in() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")
    loop.net.host("beta")
    loop.net.partition({"alpha"}, {"beta"})

    async def main() -> list[tuple[bytes, tuple[str, int]]]:
        transport_a, collector = await _bound_endpoint(alpha, 7000)
        driver_transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            _Collector, local_addr=("0.0.0.0", 7002)
        )
        driver_transport.sendto(b"hello", ("alpha", 7000))
        await asyncio.sleep(0.5)
        transport_a.close()
        driver_transport.close()
        await asyncio.sleep(0.01)
        return collector.received

    try:
        received = loop.run_until_complete(main())
    finally:
        loop.close()
    assert received == [(b"hello", ("driver", 7002))]


def test_crash_cancels_pinned_tasks_and_silences_traffic() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    cancelled: list[str] = []

    async def chatter(transport: asyncio.DatagramTransport) -> None:
        try:
            while True:
                transport.sendto(b"tick", ("alpha", 7000))
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            cancelled.append("chatter")
            raise

    async def main() -> int:
        transport_a, collector = await _bound_endpoint(alpha, 7000)
        transport_b, _ = await _bound_endpoint(beta, 7001)
        beta.create_task(chatter(transport_b))
        loop.call_later(0.35, beta.crash)
        await asyncio.sleep(2.0)
        transport_a.close()
        await asyncio.sleep(0.01)
        return len(collector.received)

    try:
        count = loop.run_until_complete(main())
    finally:
        loop.close()
    assert cancelled == ["chatter"]
    assert count == 4  # ticks at t=0, 0.1, 0.2, 0.3 — nothing after the crash
    assert loop.net._tasks["beta"] == []


def test_crash_discards_held_packets() -> None:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")

    async def swallow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5.0):
                await reader.read()
        except TimeoutError:
            pass
        finally:
            writer.close()

    async def serve() -> None:
        server = await asyncio.start_server(swallow, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def open_stream() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection("beta", 9000)

    async def main() -> None:
        beta.create_task(serve())  # dies with the crash; no reaping needed
        await asyncio.sleep(0.01)
        _, writer = await alpha.create_task(open_stream())
        loop.net.partition({"alpha"}, {"beta"})
        writer.write(b"held\n")
        await asyncio.sleep(0.1)
        loop.net.crash("beta")  # the held write is discarded here
        loop.net.heal()
        await asyncio.sleep(0.5)
        writer.close()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    labels = [e.label for e in loop.trace if e.kind == "net"]
    assert "hold alpha>beta" in labels
    assert "lost alpha>beta" in labels
    assert "release alpha>beta" not in labels


def test_crash_tears_down_the_hosts_open_connections() -> None:
    # A crashed host's own stream transports must be dropped the moment it
    # crashes, not left for the garbage collector to reap later: a transport
    # that lingers in _streams is only reachable through the cancelled task's
    # orphaned writer, whose close() then fires at GC time, so anything that
    # depended on it would vary with GC timing rather than the seed.
    loop = SimLoop(seed=0)
    hub = loop.net.host("hub")
    node = loop.net.host("node")

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5.0):
                await reader.read()
        except (TimeoutError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    async def serve() -> None:
        server = await asyncio.start_server(handler, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def dial() -> None:
        _, writer = await asyncio.open_connection("hub", 9000)
        try:
            while True:
                await asyncio.sleep(0.05)
        finally:
            writer.close()

    async def main() -> None:
        hub.create_task(serve())
        await asyncio.sleep(0.01)
        node.create_task(dial())
        await asyncio.sleep(0.05)
        assert any(key[1] == "node" for key in loop.net._streams)
        loop.net.crash("node")
        assert not any(key[1] == "node" for key in loop.net._streams)
        loop.net.crash("hub")  # reap the server side so no task lingers
        await asyncio.sleep(0.05)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_crash_guards() -> None:
    loop = SimLoop(seed=0)
    try:
        net = loop.net
        net.host("alpha")
        with pytest.raises(OSError, match="unknown host"):
            net.crash("ghost")
        with pytest.raises(ValueError, match="driver"):
            net.crash("driver")
        net.crash("alpha")
        with pytest.raises(ValueError, match="already crashed"):
            net.crash("alpha")
    finally:
        loop.close()


def test_server_tasks_are_pinned_to_the_server_not_the_dialing_client() -> None:
    # Delivery pins the receiving context: a handler task spawned when the
    # server accepts a connection must die with the server, and must survive
    # the client.
    loop = SimLoop(seed=0)
    server_host = loop.net.host("api")
    client_host = loop.net.host("cli")
    outcome: list[str] = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5.0):
                await reader.read()
            outcome.append("finished")
        except asyncio.CancelledError:
            outcome.append("cancelled")
            raise
        finally:
            writer.close()

    async def main() -> None:
        async def serve() -> None:
            server = await asyncio.start_server(handler, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(10.0)

        async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            return await asyncio.open_connection("api", 9000)

        server_host.create_task(serve())
        await asyncio.sleep(0.01)
        _, writer = await client_host.create_task(connect())
        loop.net.crash("cli")   # handler survives: it belongs to "api"
        await asyncio.sleep(0.1)
        loop.net.crash("api")   # now the handler dies
        await asyncio.sleep(0.1)
        writer.close()

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert outcome == ["cancelled"]
