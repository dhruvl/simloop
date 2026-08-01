"""The fake socket stream transports expose for client-stack introspection.

anyio reads get_extra_info("socket") and eagerly calls getpeername()/.family
on it (anyio/abc/_sockets.py); httpcore polls fileno() to decide whether a
pooled connection died. These tests pin the contract both stacks rely on.
"""

from __future__ import annotations

import asyncio
import ipaddress
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


def _connected_pair(loop: SimLoop, handler: Any = _hold_open) -> tuple[Any, Any]:
    """One established client connection; returns (client_writer, server)."""

    async def main() -> tuple[Any, Any]:
        server = await loop.net.host("server").create_task(
            asyncio.start_server(handler, "0.0.0.0", 9000)
        )
        writer_box: list[Any] = []

        async def connect() -> None:
            _, writer = await asyncio.open_connection("server", 9000)
            writer_box.append(writer)

        await loop.net.host("client").create_task(connect())
        return writer_box[0], server

    pair: tuple[Any, Any] = loop.run_until_complete(main())
    return pair


def test_stream_transport_exposes_a_socket_object() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
    sock = writer.transport.get_extra_info("socket")
    assert sock is not None
    assert sock is writer.transport.get_extra_info("socket")  # stable singleton
    assert sock.family == socket.AF_INET
    assert sock.type == socket.SOCK_STREAM
    assert sock.proto == socket.IPPROTO_TCP
    server.close()
    loop.close()


def test_socket_addresses_are_synthetic_ip_tuples() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
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
    server.close()
    loop.close()


def test_socket_options_and_teardown_calls_are_inert() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
    sock = writer.transport.get_extra_info("socket")
    assert sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True) is None
    assert sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) is None
    assert sock.shutdown(socket.SHUT_RDWR) is None
    assert sock.close() is None
    with pytest.raises(AttributeError):
        sock.recv(1)  # no pretending to be readable
    server.close()
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

    assert loop.run_until_complete(main()) is None
    loop.close()


def _readable(fd: int) -> bool:
    """The exact check httpcore's is_socket_readable performs."""
    if fd < 0:
        return True
    rready, _, _ = select.select([fd], [], [], 0)
    return bool(rready)


def test_fileno_is_lazy_and_not_readable_while_live() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
    sock = writer.transport.get_extra_info("socket")
    assert sock._park is None  # no kernel object until someone asks
    fd = sock.fileno()
    assert fd >= 0
    assert fd == sock.fileno()  # stable
    assert not _readable(fd)  # live connection: httpcore keeps pooling it
    server.close()
    writer.close()
    loop.run_until_complete(asyncio.sleep(1.0))
    loop.close()


def test_fd_turns_readable_when_the_peer_closes() -> None:
    loop = _network()

    async def close_after_a_beat(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await asyncio.sleep(0.5)  # after the client has taken its fd
        writer.close()

    writer, server = _connected_pair(loop, close_after_a_beat)
    sock = writer.transport.get_extra_info("socket")
    fd = sock.fileno()
    assert not _readable(fd)
    # Let the FIN cross the simulated network.
    loop.run_until_complete(asyncio.sleep(1.0))
    assert _readable(fd)  # httpcore now sees the connection as expired
    server.close()
    writer.close()
    loop.run_until_complete(asyncio.sleep(1.0))
    loop.close()


def test_fd_born_readable_when_the_peer_left_first() -> None:
    """A FIN can land before anyone asks for the socket."""
    loop = _network()

    async def close_immediately(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.close()

    writer, server = _connected_pair(loop, close_immediately)
    loop.run_until_complete(asyncio.sleep(1.0))  # FIN already delivered
    sock = writer.transport.get_extra_info("socket")
    assert _readable(sock.fileno())  # never claim a dead connection is live
    server.close()
    writer.close()
    loop.run_until_complete(asyncio.sleep(1.0))
    loop.close()


def test_fd_lifecycle_ends_with_the_transport() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
    sock = writer.transport.get_extra_info("socket")
    fd = sock.fileno()
    assert not _readable(fd)
    writer.close()
    loop.run_until_complete(asyncio.sleep(1.0))
    assert sock._park is None  # both ends closed, nothing leaked
    assert sock.fileno() == -1  # closed-socket semantics
    server.close()
    loop.close()


def test_fileno_after_teardown_never_creates_a_descriptor() -> None:
    loop = _network()
    writer, server = _connected_pair(loop)
    sock = writer.transport.get_extra_info("socket")
    writer.close()
    loop.run_until_complete(asyncio.sleep(1.0))
    assert sock.fileno() == -1
    assert sock._park is None
    server.close()
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


def test_sock_connect_still_fences_datagram_sockets() -> None:
    loop = _network()

    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            with pytest.raises(SimulationFenceError):
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
    loop: SimLoop, port: int = 9000
) -> Callable[[], Coroutine[Any, Any, tuple[Any, Any, Any]]]:
    """The exact two-call sequence aiohttp + aiohappyeyeballs performs."""

    async def connect() -> tuple[Any, Any, Any]:
        infos = await loop.getaddrinfo("server", port, type=socket.SOCK_STREAM)
        family, kind, proto, _, address = infos[0]
        sock = socket.socket(family=family, type=kind, proto=proto)
        sock.setblocking(False)
        await loop.sock_connect(sock, address)
        transport, protocol = await loop.create_connection(
            asyncio.Protocol, ssl=None, server_hostname=None, sock=sock
        )
        return sock, transport, protocol

    return connect


def test_parked_socket_upgrades_to_a_sim_connection() -> None:
    loop = _network()
    loop.net.set_defaults(latency=(0.001, 0.001))  # so the round trip is visible

    async def main() -> tuple[Any, Any]:
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
    loop.run_until_complete(asyncio.sleep(1.0))  # let both ends finish closing
    assert sock.fileno() == -1  # the loop took ownership and closed it
    assert elapsed > 0.0  # the handshake cost its round trip
    assert not loop._sock_targets  # the parked entry was claimed
    loop.close()


def test_refused_port_raises_from_the_upgrade() -> None:
    loop = _network()
    loop.net.set_defaults(latency=(0.001, 0.001))

    async def main() -> None:
        with pytest.raises(ConnectionRefusedError):
            await loop.net.host("client").create_task(
                _aiohttp_style_connect(loop, port=9999)()
            )

    loop.run_until_complete(main())
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
