"""The fake socket stream transports expose for client-stack introspection.

anyio reads get_extra_info("socket") and eagerly calls getpeername()/.family
on it (anyio/abc/_sockets.py); httpcore polls fileno() to decide whether a
pooled connection died. These tests pin the contract both stacks rely on.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import Any

import pytest

from simloop import SimLoop


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

    return loop.run_until_complete(main())


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
