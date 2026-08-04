"""Simulated stream connections: handshake, transfer, teardown."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from simloop import SimLoop, SimulationDeadlockError


def _network(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    return loop


async def _echo_lines(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while line := await reader.readline():
        writer.write(line.upper())
        await writer.drain()
    writer.close()
    await writer.wait_closed()


async def _reap(task: "asyncio.Task[None]") -> None:
    """Cancel a long-lived server task so nothing is left pending at stop."""
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_unmodified_streams_echo() -> None:
    loop = _network()

    async def serve() -> None:
        server = await asyncio.start_server(_echo_lines, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def request() -> list[bytes]:
        reader, writer = await asyncio.open_connection("server", 9000)
        replies = []
        for word in (b"one\n", b"two\n", b"three\n"):
            writer.write(word)
            await writer.drain()
            replies.append(await reader.readline())
        writer.close()
        await writer.wait_closed()
        return replies

    async def main() -> list[bytes]:
        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        replies: list[bytes] = await loop.net.host("client").create_task(request())
        await _reap(serve_task)
        return replies

    try:
        replies = loop.run_until_complete(main())
    finally:
        loop.close()
    assert replies == [b"ONE\n", b"TWO\n", b"THREE\n"]


def test_user_protocol_classes_run_unchanged() -> None:
    loop = _network()
    events: list[str] = []

    class Greeter(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            events.append(f"server saw {transport.get_extra_info('peername')[0]}")
            transport.write(b"hello")
            transport.close()

    class Listener(asyncio.Protocol):
        def __init__(self) -> None:
            self.done = asyncio.get_running_loop().create_future()

        def data_received(self, data: bytes) -> None:
            events.append(f"client got {data.decode()}")

        def connection_lost(self, exc: Exception | None) -> None:
            events.append(f"client lost {exc!r}")
            self.done.set_result(None)

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            await running.create_server(Greeter, "0.0.0.0", 9000)
            await asyncio.sleep(10.0)

        async def connect() -> None:
            _, protocol = await running.create_connection(Listener, "server", 9000)
            await protocol.done

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(connect())
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert events == ["server saw client", "client got hello", "client lost None"]


def test_connect_to_nothing_is_refused_after_a_round_trip() -> None:
    loop = _network()
    loop.net.set_defaults(latency=(0.05, 0.05))

    async def main() -> float:
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("server", 9999)
        return asyncio.get_running_loop().time()

    try:
        elapsed = loop.run_until_complete(main())
    finally:
        loop.close()
    assert elapsed == pytest.approx(0.1)  # syn there + refusal back


def test_a_host_can_connect_to_its_own_listener() -> None:
    # Both ends of this connection live on one machine, so the stream
    # registry must tell them apart by more than the host name — the
    # client's ephemeral port against the listener's port is what does it.
    loop = SimLoop(seed=0)
    loop.net.host("solo")

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.readline()
        writer.write(b"pong\n")
        await writer.drain()
        writer.close()

    async def main() -> bytes:
        server = await asyncio.start_server(handle, "0.0.0.0", 9000)
        reader, writer = await asyncio.open_connection("solo", 9000)
        writer.write(b"ping\n")
        await writer.drain()
        reply = await reader.readline()
        writer.close()
        await writer.wait_closed()
        server.close()
        return reply

    try:
        reply = loop.run_until_complete(loop.net.host("solo").create_task(main()))
    finally:
        loop.close()
    assert reply == b"pong\n"
    assert not loop.net._streams  # both ends existed, and both were torn down


def test_a_loopback_connect_to_a_closed_port_is_refused() -> None:
    # The refusal answers to the connector's own port. Addressed to the
    # listener's port instead, it would land on the queue the syn already
    # advanced and wait there forever — a deadlock instead of an error.
    loop = SimLoop(seed=0)
    loop.net.host("solo")

    async def main() -> None:
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("solo", 9999)

    try:
        loop.run_until_complete(loop.net.host("solo").create_task(main()))
    finally:
        loop.close()


def test_bytes_arrive_complete_and_in_order_under_latency_chaos() -> None:
    loop = _network(seed=5)
    loop.net.set_defaults(latency=(0.001, 0.2))

    async def collect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read()
        chunks.append(data)
        writer.close()

    chunks: list[bytes] = []
    payload = b"".join(f"chunk-{i:03d};".encode() for i in range(50))

    async def main() -> None:
        server = await loop.net.host("server").create_task(
            asyncio.start_server(collect, "0.0.0.0", 9000)
        )

        async def send() -> None:
            _, writer = await asyncio.open_connection("server", 9000)
            for i in range(50):
                writer.write(f"chunk-{i:03d};".encode())
            writer.close()
            await writer.wait_closed()

        await loop.net.host("client").create_task(send())
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert chunks == [payload]


def test_abort_resets_the_peer() -> None:
    loop = _network()
    lost: list[BaseException | None] = []

    class Victim(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            lost.append(exc)

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            await running.create_server(Victim, "0.0.0.0", 9000)
            await asyncio.sleep(10.0)

        async def connect_and_abort() -> None:
            transport, _ = await running.create_connection(
                asyncio.Protocol, "server", 9000
            )
            transport.abort()

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(connect_and_abort())
        await asyncio.sleep(0.5)
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert len(lost) == 1
    assert isinstance(lost[0], ConnectionResetError)


def test_server_close_clients_disconnects_the_peer() -> None:
    loop = _network()
    lost: list[BaseException | None] = []
    holder: dict[str, Any] = {}

    class Peer(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            lost.append(exc)

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            holder["server"] = await running.create_server(
                asyncio.Protocol, "0.0.0.0", 9000
            )
            await asyncio.sleep(10.0)

        async def connect() -> None:
            await running.create_connection(Peer, "server", 9000)

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(connect())
        holder["server"].close_clients()
        await asyncio.sleep(0.5)
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert lost == [None]


def test_server_abort_clients_resets_the_peer() -> None:
    loop = _network()
    lost: list[BaseException | None] = []
    holder: dict[str, Any] = {}

    class Peer(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            lost.append(exc)

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            holder["server"] = await running.create_server(
                asyncio.Protocol, "0.0.0.0", 9000
            )
            await asyncio.sleep(10.0)

        async def connect() -> None:
            await running.create_connection(Peer, "server", 9000)

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(connect())
        holder["server"].abort_clients()
        await asyncio.sleep(0.5)
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert len(lost) == 1
    assert isinstance(lost[0], ConnectionResetError)


class _Buffered(asyncio.BufferedProtocol):
    """Records what each get_buffer/buffer_updated pair was handed.

    ``room`` is how much space it offers, so a value smaller than a packet
    forces the delivery loop to fill it more than once.
    """

    def __init__(self, room: int = 4096) -> None:
        self._room = room
        self._buffer = bytearray(max(room, 1))
        self.transport: Any = None
        self.chunks: list[bytes] = []
        self.lost: list[BaseException | None] = []

    def connection_made(self, transport: Any) -> None:
        self.transport = transport

    def get_buffer(self, sizehint: int) -> memoryview:
        return memoryview(self._buffer)[: self._room]

    def buffer_updated(self, nbytes: int) -> None:
        self.chunks.append(bytes(self._buffer[:nbytes]))

    def connection_lost(self, exc: Exception | None) -> None:
        self.lost.append(exc)


def _exchange(loop: SimLoop, factory: Any, writes: list[bytes]) -> None:
    """Serve ``factory`` on host ``server`` and write each packet to it."""

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            server = await running.create_server(factory, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(10.0)

        async def send() -> None:
            transport, _ = await running.create_connection(
                asyncio.Protocol, "server", 9000
            )
            for payload in writes:
                transport.write(payload)
            await asyncio.sleep(1.0)
            transport.close()

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(send())
        await asyncio.sleep(1.0)
        await _reap(serve_task)

    loop.run_until_complete(main())


def test_a_buffered_protocol_is_fed_one_packet_at_a_time() -> None:
    loop = _network()
    protocol = _Buffered()

    try:
        _exchange(loop, lambda: protocol, [b"one", b"two"])
    finally:
        loop.close()
    assert protocol.chunks == [b"one", b"two"]


def test_a_short_buffer_is_filled_in_successive_chunks() -> None:
    loop = _network()
    protocol = _Buffered(room=2)

    try:
        _exchange(loop, lambda: protocol, [b"abcdef"])
    finally:
        loop.close()
    assert protocol.chunks == [b"ab", b"cd", b"ef"]
    assert b"".join(protocol.chunks) == b"abcdef"


def test_an_empty_buffer_is_reported_as_a_protocol_bug() -> None:
    loop = _network()
    protocol = _Buffered(room=0)

    try:
        with pytest.raises(RuntimeError, match="empty buffer"):
            _exchange(loop, lambda: protocol, [b"anything"])
    finally:
        loop.close()


def test_set_protocol_switches_to_buffered_delivery_mid_stream() -> None:
    loop = _network()
    buffered = _Buffered()
    plain: list[bytes] = []

    class Switcher(asyncio.Protocol):
        def __init__(self) -> None:
            self.transport: Any = None

        def connection_made(self, transport: Any) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            plain.append(data)
            self.transport.set_protocol(buffered)

    try:
        _exchange(loop, Switcher, [b"before", b"after"])
    finally:
        loop.close()
    assert plain == [b"before"]
    assert buffered.chunks == [b"after"]


def test_a_paused_backlog_drains_through_the_buffered_path() -> None:
    loop = _network()

    class Paused(_Buffered):
        def connection_made(self, transport: Any) -> None:
            super().connection_made(transport)
            transport.pause_reading()

    protocol = Paused()

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            server = await running.create_server(lambda: protocol, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(10.0)

        async def send() -> None:
            transport, _ = await running.create_connection(
                asyncio.Protocol, "server", 9000
            )
            transport.write(b"held")
            transport.write(b"too")
            await asyncio.sleep(1.0)
            transport.close()

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        send_task = loop.net.host("client").create_task(send())
        await asyncio.sleep(0.5)
        assert protocol.chunks == []
        protocol.transport.resume_reading()
        await send_task
        await asyncio.sleep(1.0)
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert protocol.chunks == [b"held", b"too"]


def test_force_close_reports_the_failure_and_resets_the_peer() -> None:
    loop = _network()
    failure = ConnectionAbortedError("handshake gave up")
    client_lost: list[BaseException | None] = []
    server_lost: list[BaseException | None] = []

    class Client(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            client_lost.append(exc)

    class Server(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            server_lost.append(exc)

    async def main() -> None:
        running: Any = asyncio.get_running_loop()

        async def serve() -> None:
            server = await running.create_server(Server, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(10.0)

        async def connect_and_fail() -> None:
            transport, _ = await running.create_connection(Client, "server", 9000)
            transport._force_close(failure)

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        await loop.net.host("client").create_task(connect_and_fail())
        await asyncio.sleep(0.5)
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert client_lost == [failure]
    assert len(server_lost) == 1 and isinstance(server_lost[0], ConnectionResetError)


def test_duplicate_bind_and_foreign_bind_are_rejected() -> None:
    loop = _network()

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve_twice() -> None:
            await running.create_server(asyncio.Protocol, "0.0.0.0", 9000)
            with pytest.raises(OSError, match="in use"):
                await running.create_server(asyncio.Protocol, "0.0.0.0", 9000)
            with pytest.raises(OSError, match="cannot bind"):
                await running.create_server(asyncio.Protocol, "client", 9001)

        await loop.net.host("server").create_task(serve_twice())

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_ssl_arguments_are_checked_the_way_the_stdlib_checks_them() -> None:
    import ssl

    loop = _network()

    async def main() -> None:
        running: Any = asyncio.get_running_loop()
        with pytest.raises(TypeError, match="'object'"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, ssl=object()
            )
        with pytest.raises(ValueError, match="server_hostname"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, server_hostname="server"
            )
        with pytest.raises(ValueError, match="server_hostname"):
            await running.create_connection(
                asyncio.Protocol, ssl=ssl.create_default_context(), sock=object()
            )
        with pytest.raises(ValueError, match="ssl_handshake_timeout"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, ssl_handshake_timeout=1.0
            )
        with pytest.raises(ValueError, match="ssl_shutdown_timeout"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, ssl_shutdown_timeout=1.0
            )
        with pytest.raises(ValueError, match="valid SSLContext"):
            await running.create_server(asyncio.Protocol, "0.0.0.0", 9000, ssl=True)
        with pytest.raises(ValueError, match="ssl_handshake_timeout"):
            await running.create_server(
                asyncio.Protocol, "0.0.0.0", 9000, ssl_handshake_timeout=1.0
            )

    try:
        loop.run_until_complete(loop.net.host("server").create_task(main()))
    finally:
        loop.close()


def test_the_fence_did_not_get_wider_than_tls() -> None:
    import socket
    import ssl

    from simloop import SimulationFenceError

    loop = _network()

    async def main() -> None:
        running: Any = asyncio.get_running_loop()
        context = ssl.create_default_context()
        with pytest.raises(SimulationFenceError, match="local_addr"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, local_addr=("client", 0)
            )
        with pytest.raises(SimulationFenceError, match="family"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, family=socket.AF_INET6
            )
        with pytest.raises(SimulationFenceError, match="create_datagram_endpoint"):
            await running.create_datagram_endpoint(
                asyncio.DatagramProtocol, local_addr=("0.0.0.0", 9001), ssl=context
            )

    try:
        loop.run_until_complete(loop.net.host("client").create_task(main()))
    finally:
        loop.close()


def test_server_reports_no_sockets_while_serving() -> None:
    # Server libraries read .sockets during startup to report their bound
    # address, and the stdlib documents the tuple as possibly empty — so a
    # simulated listener answers with none rather than not answering.
    loop = _network()

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> asyncio.AbstractServer:
            return await running.create_server(asyncio.Protocol, "0.0.0.0", 9000)

        server = await loop.net.host("server").create_task(serve())
        assert server.sockets == ()
        server.close()
        await server.wait_closed()
        assert server.sockets == ()

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_server_close_stops_accepting() -> None:
    loop = _network()

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> asyncio.AbstractServer:
            return await running.create_server(asyncio.Protocol, "0.0.0.0", 9000)

        server = await loop.net.host("server").create_task(serve())
        assert server.is_serving()
        server.close()
        await server.wait_closed()
        assert not server.is_serving()
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("server", 9000)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_connect_cancelled_in_accept_window_leaves_nothing_connected() -> None:
    # A timeout tuned to the accept's arrival can land in the very step that
    # builds the client transport: connection_made runs, but the connector is
    # cancelled before it is handed the transport. That half-open connection
    # must be torn down, not orphaned. Seed 2 with a round-trip-length timeout
    # deterministically lands in that window.
    loop = _network(seed=2)
    loop.net.set_defaults(latency=(0.05, 0.05))
    server_lost: list[BaseException | None] = []
    made: list[str] = []

    class Server(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            server_lost.append(exc)

    class Client(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            made.append("made")

    async def main() -> None:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            server = await running.create_server(Server, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(5.0)

        async def connect() -> None:
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.1):
                    await running.create_connection(Client, "server", 9000)

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.001)
        await loop.net.host("client").create_task(connect())
        await asyncio.sleep(1.0)  # let the reset reach the server
        await _reap(serve_task)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    assert made == ["made"]  # the accept window really was reached
    assert not any(key[1] == "client" for key in loop.net._streams)
    assert len(server_lost) == 1 and isinstance(server_lost[0], ConnectionResetError)


def test_connect_across_partition_hangs_until_timeout() -> None:
    # The syn is held before any listener lookup happens, so no server is
    # needed to observe the hang: only the connector's own timeout fires.
    loop = _network()
    loop.net.partition({"server"}, {"client"})

    async def connect() -> None:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1.0):
                await asyncio.open_connection("server", 9000)

    async def main() -> None:
        await loop.net.host("client").create_task(connect())

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_connect_across_partition_without_timeout_is_a_deadlock() -> None:
    # No timer is pending while the syn is held, so the run can never make
    # progress — exactly the missing-timeout bug this tool exists to expose.
    # Single top-level task on purpose: run_until_complete cancels and reaps
    # only its own future on the stall path, so this stays stderr-clean.
    loop = _network()
    loop.net.partition({"server"}, {"driver"})

    async def connect() -> None:
        await asyncio.open_connection("server", 9000)

    try:
        with pytest.raises(SimulationDeadlockError):
            loop.run_until_complete(connect())
    finally:
        loop.close()


def test_stream_goes_silent_under_partition_and_resumes_after_heal() -> None:
    loop = _network()

    async def main() -> bytes:
        running = asyncio.get_running_loop()

        async def serve() -> None:
            server = await asyncio.start_server(_echo_lines, "0.0.0.0", 9000)
            async with server:
                await asyncio.sleep(10.0)

        async def request() -> bytes:
            reader, writer = await asyncio.open_connection("server", 9000)
            loop.net.partition({"server"}, {"client"})
            writer.write(b"during\n")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(1.0):
                    await reader.readline()
            running.call_later(1.0, loop.net.heal)
            line = await reader.readline()  # released bytes arrive after heal
            writer.close()
            await writer.wait_closed()
            return line

        serve_task = loop.net.host("server").create_task(serve())
        await asyncio.sleep(0.01)
        line: bytes = await loop.net.host("client").create_task(request())
        await _reap(serve_task)
        await asyncio.sleep(0.01)  # let the echoed connection's own fin land
        return line

    try:
        line = loop.run_until_complete(main())
    finally:
        loop.close()
    assert line == b"DURING\n"
    labels = [e.label for e in loop.trace if e.kind == "net"]
    assert "hold client>server" in labels
    assert "release client>server" in labels
