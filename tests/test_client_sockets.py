"""The fake socket stream transports expose for client-stack introspection.

anyio reads get_extra_info("socket") and eagerly calls getpeername()/.family
on it (anyio/abc/_sockets.py); httpcore polls fileno() to decide whether a
pooled connection died. These tests pin the contract both stacks rely on.
"""

from __future__ import annotations

import asyncio
import gc
import ipaddress
import os
import select
import socket
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from simloop import SimLoop, SimulationFenceError


def _network(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    return loop


async def _hold_open(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.read()  # stay open until the client closes
    writer.close()


def _connected_pair(loop: SimLoop, handler: Any = _hold_open) -> tuple[Any, Any, Any]:
    """One established client connection.

    Returns (client_writer, accepted_writer, server): the two ends of the same
    connection plus the listening server.
    """

    accepted: list[Any] = []

    async def accept(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        accepted.append(writer)
        await handler(reader, writer)

    async def main() -> tuple[Any, Any, Any]:
        server = await loop.net.host("server").create_task(
            asyncio.start_server(accept, "0.0.0.0", 9000)
        )
        writer_box: list[Any] = []

        async def connect() -> None:
            _, writer = await asyncio.open_connection("server", 9000)
            writer_box.append(writer)

        await loop.net.host("client").create_task(connect())
        return writer_box[0], accepted[0], server

    trio: tuple[Any, Any, Any] = loop.run_until_complete(main())
    return trio


def _settle(loop: SimLoop) -> None:
    """Let queued closes cross the simulated network before teardown."""
    loop.run_until_complete(asyncio.sleep(1.0))


def test_stream_transport_exposes_a_socket_object() -> None:
    loop = _network()
    try:
        writer, _, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        assert sock is not None
        assert sock is writer.transport.get_extra_info("socket")  # stable singleton
        assert sock.family == socket.AF_INET
        assert sock.type == socket.SOCK_STREAM
        assert sock.proto == socket.IPPROTO_TCP
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_socket_addresses_are_synthetic_ip_tuples() -> None:
    loop = _network()
    try:
        writer, accepted, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        local_ip, local_port = sock.getsockname()
        peer_ip, peer_port = sock.getpeername()
        # Plausible AF_INET sockaddrs: ip_address() must accept them.
        ipaddress.ip_address(local_ip)
        ipaddress.ip_address(peer_ip)
        assert local_ip == loop.net.address("client")
        assert peer_ip == loop.net.address("server")
        assert peer_port == 9000
        # Same endpoints the transport itself reports, by name.
        assert writer.transport.get_extra_info("peername") == ("server", 9000)
        assert writer.transport.get_extra_info("sockname")[1] == local_port
        # The accepting side sees the client from the other direction: the two
        # fake sockets describe one connection, not two unrelated ones.
        accepted_sock = accepted.transport.get_extra_info("socket")
        assert accepted_sock.getpeername() == (local_ip, local_port)
        assert accepted_sock.getsockname() == (peer_ip, 9000)
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_socket_options_and_teardown_calls_are_inert() -> None:
    loop = _network()
    try:
        writer, _, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        assert sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True) is None
        assert sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) is None
        assert sock.shutdown(socket.SHUT_RDWR) is None
        assert sock.close() is None
        with pytest.raises(AttributeError):
            sock.recv(1)  # no pretending to be readable
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_datagram_transports_still_answer_none() -> None:
    loop = _network()

    async def main() -> Any:
        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("0.0.0.0", 5000)
        )
        try:
            return transport.get_extra_info("socket")
        finally:
            transport.close()

    try:
        assert loop.run_until_complete(main()) is None
    finally:
        loop.close()


def _readable(fd: int) -> bool:
    """The exact check httpcore's is_socket_readable performs."""
    if fd < 0:
        return True
    rready, _, _ = select.select([fd], [], [], 0)
    return bool(rready)


def test_fileno_is_lazy_and_not_readable_while_live() -> None:
    loop = _network()
    try:
        writer, _, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        assert sock._park is None  # no kernel object until someone asks
        fd = sock.fileno()
        assert fd >= 0
        assert fd == sock.fileno()  # stable
        assert not _readable(fd)  # live connection: httpcore keeps pooling it
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_fd_turns_readable_when_the_peer_closes() -> None:
    loop = _network()

    async def close_after_a_beat(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await asyncio.sleep(0.5)  # after the client has taken its fd
        writer.close()

    try:
        writer, _, server = _connected_pair(loop, close_after_a_beat)
        sock = writer.transport.get_extra_info("socket")
        fd = sock.fileno()
        assert not _readable(fd)
        # Let the FIN cross the simulated network.
        _settle(loop)
        assert _readable(fd)  # httpcore now sees the connection as expired
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_fd_born_readable_when_the_peer_left_first() -> None:
    """A FIN can land before anyone asks for the socket."""
    loop = _network()

    async def close_immediately(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.close()

    try:
        writer, _, server = _connected_pair(loop, close_immediately)
        _settle(loop)  # FIN already delivered
        sock = writer.transport.get_extra_info("socket")
        assert sock.fileno() >= 0  # the local end is still open: a real fd
        assert _readable(sock.fileno())  # never claim a dead connection is live
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_fd_lifecycle_ends_with_the_transport() -> None:
    loop = _network()
    try:
        writer, _, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        fd = sock.fileno()
        assert not _readable(fd)
        writer.close()
        _settle(loop)
        assert sock._park is None  # both ends closed, nothing leaked
        assert sock.fileno() == -1  # closed-socket semantics
        server.close()
    finally:
        loop.close()


def test_reset_closes_the_descriptor_instead_of_arming_it() -> None:
    """An aborting peer tears the connection down; the fd goes, not readable."""
    loop = _network()

    async def abort_after_a_beat(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await asyncio.sleep(0.5)  # after the client has taken its fd
        writer.transport.abort()

    try:
        writer, _, server = _connected_pair(loop, abort_after_a_beat)
        sock = writer.transport.get_extra_info("socket")
        assert sock.fileno() >= 0
        _settle(loop)  # let the RST cross the network
        assert sock.fileno() == -1  # the reset finished the connection outright
        assert sock._park is None
        assert _readable(sock.fileno())  # a poll still reads it as dead
        server.close()
        writer.close()
        _settle(loop)
    finally:
        loop.close()


def test_fileno_after_teardown_never_creates_a_descriptor() -> None:
    loop = _network()
    try:
        writer, _, server = _connected_pair(loop)
        sock = writer.transport.get_extra_info("socket")
        writer.close()
        _settle(loop)
        assert sock.fileno() == -1
        assert sock._park is None
        server.close()
    finally:
        loop.close()


def test_sock_connect_records_a_stream_socket_target() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)  # what aiohappyeyeballs does
        try:
            await loop.sock_connect(sock, (loop.net.address("server"), 9000))
            assert loop._sock_targets[sock] == (loop.net.address("server"), 9000)
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


def test_sock_connect_rejects_unknown_addresses() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="10.9.9.9"):
                await loop.sock_connect(sock, ("10.9.9.9", 9000))
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


@pytest.mark.parametrize(
    "address",
    [
        "server",  # a bare string is a sequence of characters, not an endpoint
        ("server",),  # one item short
        ("server", "9000"),  # a port that only looks like a number
    ],
)
def test_sock_connect_rejects_malformed_addresses(address: Any) -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="host, port"):
                await loop.sock_connect(sock, address)
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


def test_sock_connect_still_fences_datagram_sockets() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            with pytest.raises(SimulationFenceError, match="sock_connect"):
                await loop.sock_connect(sock, (loop.net.address("server"), 9000))
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


def test_sock_connect_consumes_no_virtual_time() -> None:
    loop = _network()

    async def main() -> float:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            before = loop.time()
            await loop.sock_connect(sock, ("server", 9000))
            return loop.time() - before
        finally:
            sock.close()

    assert loop.run_until_complete(main()) == 0.0
    loop.close()


def _aiohttp_style_connect(
    loop: SimLoop, port: int = 9000, sockets: list[Any] | None = None
) -> Callable[[], Coroutine[Any, Any, tuple[Any, Any, Any]]]:
    """The exact two-call sequence aiohttp + aiohappyeyeballs performs."""

    async def connect() -> tuple[Any, Any, Any]:
        infos = await loop.getaddrinfo("server", port, type=socket.SOCK_STREAM)
        family, kind, proto, _, address = infos[0]
        sock = socket.socket(family=family, type=kind, proto=proto)
        sock.setblocking(False)
        if sockets is not None:
            sockets.append(sock)  # so a refused connect can be inspected too
        await loop.sock_connect(sock, address)
        transport, protocol = await loop.create_connection(
            asyncio.Protocol, ssl=None, server_hostname=None, sock=sock
        )
        return sock, transport, protocol

    return connect


def test_parked_socket_upgrades_to_a_sim_connection() -> None:
    loop = _network()
    loop.net.set_defaults(latency=(0.001, 0.001))  # so the round trip is visible

    async def main() -> tuple[Any, float]:
        server = await loop.net.host("server").create_task(
            asyncio.start_server(_hold_open, "0.0.0.0", 9000)
        )
        started = loop.time()
        sock, transport, _ = await loop.net.host("client").create_task(
            _aiohttp_style_connect(loop)()
        )
        elapsed = loop.time() - started
        transport.close()
        server.close()
        return sock, elapsed

    sock, elapsed = loop.run_until_complete(main())
    _settle(loop)  # let both ends finish closing
    assert sock.fileno() == -1  # the loop took ownership and closed it
    assert elapsed == pytest.approx(0.002)  # SYN out, accept back: one round trip
    assert not loop._sock_targets  # the parked entry was claimed
    loop.close()


def test_refused_port_raises_from_the_upgrade() -> None:
    loop = _network()
    loop.net.set_defaults(latency=(0.001, 0.001))
    sockets: list[Any] = []

    async def main() -> float:
        started = loop.time()
        with pytest.raises(ConnectionRefusedError):
            await loop.net.host("client").create_task(
                _aiohttp_style_connect(loop, port=9999, sockets=sockets)()
            )
        return loop.time() - started

    elapsed = loop.run_until_complete(main())
    # Refusal costs what a real one does: SYN out, refusal back.
    assert elapsed == pytest.approx(0.002)
    assert sockets[0].fileno() == -1  # the loop owned it and closed it anyway
    loop.close()


def test_unparked_socket_is_rejected() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="sock_connect"):
                await loop.create_connection(asyncio.Protocol, sock=sock)
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


def test_sock_and_host_together_follow_stdlib_rules() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            await loop.sock_connect(sock, ("server", 9000))
            with pytest.raises(ValueError):
                await loop.create_connection(
                    asyncio.Protocol, "server", 9000, sock=sock
                )
        finally:
            sock.close()

    loop.run_until_complete(main())
    loop.close()


def test_closing_the_loop_releases_parked_descriptors() -> None:
    """A pooling client leaves its connections open; the run still ends clean."""
    loop = _network()

    async def main() -> tuple[Any, Any, Any]:
        # A bare protocol server, not start_server: the connection has to
        # survive to the end of the run, and a stream handler parked on the
        # other side of it would be a leak of its own.
        server = await loop.net.host("server").create_task(
            loop.create_server(asyncio.Protocol, "0.0.0.0", 9000)
        )
        _, transport, _ = await loop.net.host("client").create_task(
            _aiohttp_style_connect(loop)()
        )
        return server, transport, transport.get_extra_info("socket")

    server, transport, sock = loop.run_until_complete(main())
    fd = sock.fileno()
    assert fd >= 0
    server.close()
    loop.close()  # the connection is still open, exactly as a pool leaves it
    assert sock._park is None
    with pytest.raises(OSError):
        os.fstat(fd)  # the descriptor went back to the OS, not just its owner
    gc.collect()  # under -W error::ResourceWarning: nothing left to complain
    assert transport.get_extra_info("socket") is sock


class _Collect(asyncio.Protocol):
    """A protocol that keeps what it was sent, so replies are observable."""

    def __init__(self) -> None:
        self.data = bytearray()

    def data_received(self, data: bytes) -> None:
        self.data += data


async def _echo_once(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(await reader.read(64))
    await writer.drain()
    writer.close()


async def _client_stack_probe(loop: SimLoop, name: str) -> None:
    """One aiohttp-shaped request: resolve, connect, introspect, write, close."""
    infos = await loop.getaddrinfo("server", 9000, type=socket.SOCK_STREAM)
    family, kind, proto, _, address = infos[0]
    sock = socket.socket(family=family, type=kind, proto=proto)
    sock.setblocking(False)
    await loop.sock_connect(sock, address)
    transport, _ = await loop.create_connection(_Collect, sock=sock)
    fake = transport.get_extra_info("socket")
    fake.getpeername()
    fake.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    fake.fileno()  # the descriptor a pool would poll
    transport.write(name.encode())
    await asyncio.sleep(0.05)
    transport.close()


def _run_client_stack(seed: int) -> str:
    """Three concurrent clients driving the whole new surface; trace hash out."""
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.set_defaults(latency=(0.001, 0.005))

    async def main() -> None:
        server = await loop.net.host("server").create_task(
            asyncio.start_server(_echo_once, "0.0.0.0", 9000)
        )
        clients = [
            loop.net.host(name).create_task(_client_stack_probe(loop, name))
            for name in ("alice", "bob", "carol")
        ]
        for client in clients:
            await client
        server.close()

    try:
        loop.run_until_complete(main())
        loop.run_until_complete(asyncio.sleep(1.0))
        return loop.trace_hash()
    finally:
        loop.close()


def test_client_stack_path_is_deterministic() -> None:
    for seed in range(3):
        hashes = {_run_client_stack(seed) for _ in range(2)}
        assert len(hashes) == 1, f"seed {seed} produced diverging traces"


def test_client_stack_path_still_varies_with_the_seed() -> None:
    # The fake socket's descriptors are OS-assigned and deliberately absent
    # from the trace, so only the schedule and the latency draws move.
    hashes = {_run_client_stack(seed) for seed in range(10)}
    assert len(hashes) >= 8
