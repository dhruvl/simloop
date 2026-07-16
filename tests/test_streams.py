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


def test_ssl_arguments_are_fenced() -> None:
    from simloop import SimulationFenceError

    loop = _network()

    async def main() -> None:
        running: Any = asyncio.get_running_loop()
        with pytest.raises(SimulationFenceError, match="create_connection"):
            await running.create_connection(
                asyncio.Protocol, "server", 9000, ssl=object()
            )

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
