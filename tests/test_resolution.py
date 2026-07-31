"""Name resolution against the simulated host table."""

from __future__ import annotations

import asyncio
import socket
from typing import Any

import pytest

from simloop import SimLoop


def _network(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    return loop


def _resolve(*args: Any, **kwargs: Any) -> Any:
    """One getaddrinfo call on a fresh network, from the driver host."""
    loop = _network()

    async def main() -> Any:
        return await asyncio.get_running_loop().getaddrinfo(*args, **kwargs)

    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


def _sockaddr(row: Any) -> tuple[str, int]:
    """The (address, port) field of one addrinfo row."""
    address, port = row[4]
    return (address, port)


async def _echo_line(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    while line := await reader.readline():
        writer.write(line.upper())
        await writer.drain()
    writer.close()
    await writer.wait_closed()


# ----------------------------------------------------------------------
# The address table
# ----------------------------------------------------------------------


def test_addresses_are_assigned_in_registration_order() -> None:
    loop = SimLoop(seed=0)
    try:
        net = loop.net
        assert net.address("driver") == "10.7.0.1"  # the network registers it first
        net.host("alpha")
        net.host("beta")
        assert net.address("alpha") == "10.7.0.2"
        assert net.address("beta") == "10.7.0.3"
        net.host("alpha")  # a repeat registration never reassigns
        assert net.address("alpha") == "10.7.0.2"
        assert net.hostname("10.7.0.2") == "alpha"
        assert net.hostname("10.7.0.3") == "beta"
    finally:
        loop.close()


def test_registration_order_decides_addresses_not_name_order() -> None:
    loop = SimLoop(seed=0)
    try:
        loop.net.host("beta")
        loop.net.host("alpha")
        assert loop.net.address("beta") == "10.7.0.2"
        assert loop.net.address("alpha") == "10.7.0.3"
    finally:
        loop.close()


def test_every_host_owns_exactly_one_address() -> None:
    loop = SimLoop(seed=0)
    try:
        names = ("alpha", "beta", "gamma", "delta")
        for name in names:
            loop.net.host(name)
        addresses = [loop.net.address(name) for name in names]
        assert len(set(addresses)) == len(names)
        assert [loop.net.hostname(address) for address in addresses] == list(names)
    finally:
        loop.close()


def test_unknown_names_and_addresses_are_rejected() -> None:
    loop = SimLoop(seed=0)
    try:
        with pytest.raises(OSError, match="unknown host"):
            loop.net.address("nowhere")
        with pytest.raises(OSError, match="unknown address"):
            loop.net.hostname("10.7.9.9")
    finally:
        loop.close()


# ----------------------------------------------------------------------
# getaddrinfo
# ----------------------------------------------------------------------


def test_getaddrinfo_returns_both_socket_kinds() -> None:
    assert _resolve("server", 9000) == [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            "",
            ("10.7.0.2", 9000),
        ),
        (
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP,
            "",
            ("10.7.0.2", 9000),
        ),
    ]


def test_getaddrinfo_rows_match_the_stdlib_tuple_shape() -> None:
    # A numeric reference lookup: this asserts the tuple layout, and never
    # asks the resolver a question that could reach the network.
    reference = socket.getaddrinfo(
        "127.0.0.1", 9000, socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP
    )[0]
    row = _resolve("server", 9000, family=socket.AF_INET, type=socket.SOCK_STREAM)[0]
    assert len(row) == len(reference) == 5
    assert row[0] == reference[0] == socket.AF_INET
    assert row[1] == reference[1] == socket.SOCK_STREAM
    assert row[2] == reference[2] == socket.IPPROTO_TCP
    assert row[3] == reference[3] == ""
    assert len(row[4]) == len(reference[4]) == 2
    assert isinstance(row[4][0], str) and isinstance(row[4][1], int)


def test_getaddrinfo_resolves_a_synthetic_address_to_itself() -> None:
    assert _resolve("10.7.0.2", 9000, type=socket.SOCK_STREAM)[0][4] == (
        "10.7.0.2",
        9000,
    )


def test_getaddrinfo_honors_type_family_and_proto_filters() -> None:
    assert [row[1] for row in _resolve("server", 80, type=socket.SOCK_STREAM)] == [
        socket.SOCK_STREAM
    ]
    assert [row[1] for row in _resolve("server", 80, type=socket.SOCK_DGRAM)] == [
        socket.SOCK_DGRAM
    ]
    assert [row[1] for row in _resolve("server", 80, proto=socket.IPPROTO_UDP)] == [
        socket.SOCK_DGRAM
    ]
    assert len(_resolve("server", 80, family=socket.AF_INET)) == 2


def test_getaddrinfo_accepts_numeric_service_forms() -> None:
    assert _resolve("server", "9000")[0][4] == ("10.7.0.2", 9000)
    assert _resolve("server", None)[0][4] == ("10.7.0.2", 0)


def test_loopback_names_resolve_to_the_calling_host() -> None:
    loop = _network()

    async def resolve_local() -> list[str]:
        running = asyncio.get_running_loop()
        found: list[str] = []
        for name in ("localhost", "127.0.0.1", "0.0.0.0", None):
            rows = await running.getaddrinfo(name, 9000, type=socket.SOCK_STREAM)
            found.append(_sockaddr(rows[0])[0])
        return found

    async def main() -> list[str]:
        result: list[str] = await loop.net.host("client").create_task(resolve_local())
        return result

    try:
        found = loop.run_until_complete(main())
    finally:
        loop.close()
    assert found == ["10.7.0.3"] * 4  # the client host, not the driver


def test_unresolvable_requests_raise_gaierror() -> None:
    unresolvable: tuple[tuple[tuple[Any, ...], dict[str, Any]], ...] = (
        (("nowhere", 9000), {}),  # a name no host registered
        (("10.7.9.9", 9000), {}),  # an address in range but unassigned
        (("server", 9000), {"family": socket.AF_INET6}),
        (("server", 9000), {"type": socket.SOCK_RAW}),
        (("server", "http"), {}),  # service names are not resolved
    )
    for args, kwargs in unresolvable:
        with pytest.raises(socket.gaierror) as raised:
            _resolve(*args, **kwargs)
        assert raised.value.errno == socket.EAI_NONAME


# ----------------------------------------------------------------------
# getnameinfo
# ----------------------------------------------------------------------


def test_getnameinfo_round_trips_a_resolved_address() -> None:
    loop = _network()

    async def main() -> list[tuple[str, str]]:
        running = asyncio.get_running_loop()
        rows = await running.getaddrinfo("server", 9000, type=socket.SOCK_STREAM)
        address = _sockaddr(rows[0])
        return [
            await running.getnameinfo(address),
            await running.getnameinfo(("server", 9000)),
            await running.getnameinfo(address, socket.NI_NUMERICHOST),
        ]

    try:
        by_address, by_name, numeric = loop.run_until_complete(main())
    finally:
        loop.close()
    assert by_address == ("server", "9000")
    assert by_name == ("server", "9000")
    assert numeric == ("10.7.0.2", "9000")


def test_getnameinfo_rejects_addresses_outside_the_simulation() -> None:
    loop = _network()

    async def main() -> None:
        await asyncio.get_running_loop().getnameinfo(("8.8.8.8", 53))

    try:
        with pytest.raises(socket.gaierror) as raised:
            loop.run_until_complete(main())
    finally:
        loop.close()
    assert raised.value.errno == socket.EAI_NONAME


# ----------------------------------------------------------------------
# Addresses as endpoints
# ----------------------------------------------------------------------


def test_streams_connect_to_a_synthetic_address() -> None:
    loop = _network()

    async def serve() -> None:
        # Binding by the host's own address is as valid as binding by name.
        server = await asyncio.start_server(_echo_line, "10.7.0.2", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def request() -> bytes:
        reader, writer = await asyncio.open_connection("10.7.0.2", 9000)
        writer.write(b"ping\n")
        await writer.drain()
        reply = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return reply

    async def main() -> bytes:
        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        reply: bytes = await loop.net.host("client").create_task(request())
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
        return reply

    try:
        assert loop.run_until_complete(main()) == b"PING\n"
    finally:
        loop.close()


def test_a_client_resolves_then_connects_to_what_it_got_back() -> None:
    loop = _network()

    async def serve() -> None:
        server = await asyncio.start_server(_echo_line, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def request() -> bytes:
        running = asyncio.get_running_loop()
        rows = await running.getaddrinfo("server", 9000, type=socket.SOCK_STREAM)
        host, port = _sockaddr(rows[0])
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"resolved\n")
        await writer.drain()
        reply = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return reply

    async def main() -> bytes:
        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        reply: bytes = await loop.net.host("client").create_task(request())
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
        return reply

    try:
        assert loop.run_until_complete(main()) == b"RESOLVED\n"
    finally:
        loop.close()


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[tuple[bytes, tuple[str, int]]] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append((data, addr))


def test_datagrams_are_sent_to_a_synthetic_address() -> None:
    loop = _network()
    collector = _Collector()

    async def bind() -> asyncio.DatagramTransport:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: collector, local_addr=("0.0.0.0", 7000)
        )
        return transport

    async def send() -> asyncio.DatagramTransport:
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("0.0.0.0", 7001)
        )
        transport.sendto(b"ping", ("10.7.0.2", 7000))
        return transport

    async def main() -> None:
        server = loop.net.host("server")
        client = loop.net.host("client")
        listening = await server.create_task(bind())
        sending = await client.create_task(send())
        await asyncio.sleep(0.1)
        listening.close()
        sending.close()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    # Packets keep host identity: the sender is reported by name.
    assert collector.received == [(b"ping", ("client", 7001))]


# ----------------------------------------------------------------------
# Determinism
# ----------------------------------------------------------------------


def _resolving_workload(seed: int) -> str:
    loop = _network(seed)
    loop.net.set_defaults(latency=(0.001, 0.02))

    async def serve() -> None:
        server = await asyncio.start_server(_echo_line, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def request(number: int) -> bytes:
        running = asyncio.get_running_loop()
        rows = await running.getaddrinfo("server", 9000, type=socket.SOCK_STREAM)
        host, port = _sockaddr(rows[0])
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(f"line{number}\n".encode())
        await writer.drain()
        reply = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return reply

    async def main() -> None:
        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        client = loop.net.host("client")
        replies = await asyncio.gather(
            *(client.create_task(request(number)) for number in range(3))
        )
        assert replies == [b"LINE0\n", b"LINE1\n", b"LINE2\n"]
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    return loop.trace_hash()


def test_a_resolving_workload_replays_identically() -> None:
    assert _resolving_workload(7) == _resolving_workload(7)


def test_resolution_records_no_trace_event() -> None:
    loop = _network()

    async def main() -> None:
        await asyncio.get_running_loop().getaddrinfo("server", 9000)

    async def nothing() -> None:
        return None

    try:
        loop.run_until_complete(main())
        resolving = loop.trace_hash()
    finally:
        loop.close()

    bare = _network()
    try:
        bare.run_until_complete(nothing())
        quiet = bare.trace_hash()
    finally:
        bare.close()
    assert resolving == quiet
