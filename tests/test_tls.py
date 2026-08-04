"""TLS inside the simulation: real handshakes over simulated packets.

Every certificate here is minted in memory for a sim hostname, and every
client context verifies against that authority alone — so a passing test says
OpenSSL really agreed, not that verification was turned off.
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import time
from typing import Any

import pytest

import _tls_certs
from simloop import SimLoop


def _network(
    seed: int = 0, latency: tuple[float, float] = (0.001, 0.001)
) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("api")
    loop.net.host("client")
    loop.net.set_defaults(latency=latency)
    return loop


def _settle(loop: SimLoop) -> None:
    """Let queued closes cross the simulated network before teardown."""
    loop.run_until_complete(asyncio.sleep(1.0))


async def _echo_once(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write((await reader.read(100)).upper())
    await writer.drain()
    writer.close()


def _serve(loop: SimLoop, handler: Any = _echo_once, port: int = 443) -> Any:
    async def listen() -> Any:
        return await asyncio.start_server(
            handler, "0.0.0.0", port, ssl=_tls_certs.server_context("api")
        )

    return loop.net.host("api").create_task(listen())


def test_a_verified_handshake_carries_an_echo() -> None:
    loop = _network()

    async def main() -> tuple[bytes, Any]:
        server = await _serve(loop)

        async def request() -> tuple[bytes, Any]:
            reader, writer = await asyncio.open_connection(
                "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
            )
            cipher = writer.transport.get_extra_info("cipher")
            writer.write(b"hello")
            await writer.drain()
            reply = await reader.read(100)
            writer.close()
            await writer.wait_closed()
            return reply, cipher

        result: tuple[bytes, Any] = await loop.net.host("client").create_task(
            request()
        )
        server.close()
        await server.wait_closed()
        return result

    try:
        reply, cipher = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert reply == b"HELLO"
    assert cipher[1] == "TLSv1.3"


def test_extra_info_answers_from_both_layers() -> None:
    loop = _network()
    context = _tls_certs.client_context()

    async def main() -> dict[str, Any]:
        server = await _serve(loop)

        async def request() -> dict[str, Any]:
            _, writer = await asyncio.open_connection(
                "api", 443, ssl=context, server_hostname="api"
            )
            transport = writer.transport
            info = {
                name: transport.get_extra_info(name)
                for name in (
                    "ssl_object",
                    "peercert",
                    "cipher",
                    "sslcontext",
                    "peername",
                    "sockname",
                    "socket",
                )
            }
            writer.close()
            await writer.wait_closed()
            return info

        info: dict[str, Any] = await loop.net.host("client").create_task(request())
        server.close()
        await server.wait_closed()
        return info

    try:
        info = loop.run_until_complete(main())
        _settle(loop)
        # The TLS layer answers for itself...
        assert isinstance(info["ssl_object"], ssl.SSLObject)
        assert info["peercert"]["subjectAltName"] == (("DNS", "api"),)
        assert len(info["cipher"]) == 3
        assert info["sslcontext"] is context
        # ...and everything else falls through to the simulated transport.
        assert info["peername"] == ("api", 443)
        assert info["sockname"][0] == "client"
        assert info["socket"].getpeername() == (loop.net.address("api"), 443)
    finally:
        loop.close()


def test_a_tls_connect_costs_two_round_trips() -> None:
    loop = _network()

    async def main() -> float:
        server = await _serve(loop)

        async def request() -> float:
            started = asyncio.get_running_loop().time()
            _, writer = await asyncio.open_connection(
                "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
            )
            elapsed = asyncio.get_running_loop().time() - started
            writer.close()
            return elapsed

        elapsed: float = await loop.net.host("client").create_task(request())
        server.close()
        await server.wait_closed()
        return elapsed

    try:
        elapsed = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    # syn + accept, then ClientHello + the server's flight.
    assert elapsed == pytest.approx(0.004)


def _upgrade_writer(
    writer: asyncio.StreamWriter, context: ssl.SSLContext, **kwargs: Any
) -> Any:
    """The STARTTLS recipe: upgrade in place and rebind the writer."""

    async def upgrade() -> Any:
        running: Any = asyncio.get_running_loop()
        transport = writer.transport
        new = await running.start_tls(
            transport, transport.get_protocol(), context, **kwargs
        )
        holder: Any = writer
        holder._transport = new
        return new

    return upgrade()


@pytest.mark.parametrize("linger", [0.0, 0.05])
def test_start_tls_upgrades_an_established_connection(linger: float) -> None:
    # With linger, the server dawdles after its plaintext reply, so the
    # ClientHello lands in the stream reader's own buffer before the upgrade
    # runs: those bytes must reach the handshake rather than be lost.
    loop = _network()
    buffered: list[int] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        assert await reader.readline() == b"STARTTLS\n"
        writer.write(b"GO AHEAD\n")
        await writer.drain()
        await asyncio.sleep(linger)
        protocol: Any = writer.transport.get_protocol()
        buffered.append(len(protocol._stream_reader._buffer))
        await _upgrade_writer(
            writer, _tls_certs.server_context("api"), server_side=True
        )
        writer.write((await reader.readline()).upper())
        await writer.drain()
        writer.close()

    async def main() -> tuple[bytes, Any]:
        async def listen() -> Any:
            return await asyncio.start_server(handle, "0.0.0.0", 443)

        server = await loop.net.host("api").create_task(listen())

        async def request() -> tuple[bytes, Any]:
            reader, writer = await asyncio.open_connection("api", 443)
            writer.write(b"STARTTLS\n")
            await writer.drain()
            assert await reader.readline() == b"GO AHEAD\n"
            upgraded = await _upgrade_writer(
                writer, _tls_certs.client_context(), server_hostname="api"
            )
            cipher = upgraded.get_extra_info("cipher")
            writer.write(b"secret\n")
            await writer.drain()
            reply = await reader.readline()
            writer.close()
            await writer.wait_closed()
            return reply, cipher

        result: tuple[bytes, Any] = await loop.net.host("client").create_task(
            request()
        )
        server.close()
        await server.wait_closed()
        return result

    try:
        reply, cipher = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert reply == b"SECRET\n"
    assert cipher[1] == "TLSv1.3"
    assert (buffered[0] > 0) is (linger > 0.0)


def test_a_certificate_for_another_name_is_rejected() -> None:
    loop = _network()

    async def main() -> BaseException:
        server = await _serve(loop, handler=_hold_open)

        async def request() -> BaseException:
            with pytest.raises(ssl.SSLCertVerificationError) as caught:
                await asyncio.open_connection(
                    "api", 443, ssl=_tls_certs.client_context(), server_hostname="evil"
                )
            return caught.value

        failure: BaseException = await loop.net.host("client").create_task(
            request()
        )
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return failure

    try:
        failure = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert isinstance(failure, ssl.CertificateError)
    assert "Hostname mismatch" in str(failure)


async def _hold_open(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        await reader.read()
    except ConnectionResetError:
        pass


def test_a_silent_peer_aborts_the_handshake_at_its_deadline() -> None:
    loop = _network()

    async def main() -> tuple[str, float]:
        async def listen() -> Any:
            return await asyncio.start_server(_hold_open, "0.0.0.0", 443)

        server = await loop.net.host("api").create_task(listen())

        async def request() -> tuple[str, float]:
            running = asyncio.get_running_loop()
            started = running.time()
            with pytest.raises(ConnectionAbortedError) as caught:
                await asyncio.open_connection(
                    "api",
                    443,
                    ssl=_tls_certs.client_context(),
                    server_hostname="api",
                    ssl_handshake_timeout=5.0,
                )
            return str(caught.value), running.time() - started

        result: tuple[str, float] = await loop.net.host("client").create_task(
            request()
        )
        server.close()
        await server.wait_closed()
        return result

    wall = time.monotonic()
    try:
        message, elapsed = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert "SSL handshake is taking longer than 5.0 seconds" in message
    # The deadline is an ordinary loop timer, so it costs five virtual
    # seconds and no wall-clock ones.
    assert 4.99 < elapsed < 5.1
    assert time.monotonic() - wall < 2.0


def test_a_plaintext_answer_fails_the_handshake_at_once() -> None:
    loop = _network()

    async def talk(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await asyncio.sleep(0.001)
        writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()

    async def main() -> tuple[str, float]:
        async def listen() -> Any:
            return await asyncio.start_server(talk, "0.0.0.0", 443)

        server = await loop.net.host("api").create_task(listen())

        async def request() -> tuple[str, float]:
            running = asyncio.get_running_loop()
            started = running.time()
            with pytest.raises(ssl.SSLError) as caught:
                await asyncio.open_connection(
                    "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
                )
            return str(caught.value), running.time() - started

        result: tuple[str, float] = await loop.net.host("client").create_task(
            request()
        )
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return result

    try:
        message, elapsed = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert "WRONG_VERSION_NUMBER" in message
    assert elapsed < 1.0  # the answer, not the sixty-second deadline


def test_closing_a_tls_connection_exchanges_close_notify() -> None:
    loop = _network()
    seen: list[Any] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            seen.append(("read", await reader.read()))
        except BaseException as exc:  # pragma: no cover - a clean close has none
            seen.append(("error", type(exc).__name__))

    async def main() -> tuple[bool, Any]:
        server = await _serve(loop, handler=handle)

        async def request() -> tuple[bool, Any]:
            _, writer = await asyncio.open_connection(
                "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
            )
            # TLS has no half-close, so the stdlib's transport refuses one.
            answer = writer.can_write_eof()
            with pytest.raises(NotImplementedError):
                writer.write_eof()
            writer.close()
            await writer.wait_closed()
            return answer, None

        answer, _ = await loop.net.host("client").create_task(request())
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return answer, None

    try:
        answer, _ = loop.run_until_complete(main())
        _settle(loop)
        assert answer is False
        assert seen == [("read", b"")]  # a clean EOF, not a reset
        assert not loop.net._streams  # both raw transports are gone
    finally:
        loop.close()


def test_a_peer_that_resets_mid_handshake_is_reported_as_a_reset() -> None:
    loop = _network()

    class Rude(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            transport.abort()

    async def main() -> str:
        running: Any = asyncio.get_running_loop()
        server = await loop.net.host("api").create_task(
            running.create_server(Rude, "0.0.0.0", 443)
        )

        async def request() -> str:
            with pytest.raises(ConnectionResetError) as caught:
                await asyncio.open_connection(
                    "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
                )
            return str(caught.value)

        message: str = await loop.net.host("client").create_task(request())
        server.close()
        await server.wait_closed()
        return message

    try:
        message = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert message == "Connection reset by peer"


def test_a_peer_that_closes_mid_handshake_is_reported_as_a_bare_reset() -> None:
    # sslproto completes an unfinished handshake with the ConnectionResetError
    # *class*, so the caller gets an instance carrying no message at all.
    loop = _network()

    class Polite(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            transport.close()

    async def main() -> str:
        running: Any = asyncio.get_running_loop()
        server = await loop.net.host("api").create_task(
            running.create_server(Polite, "0.0.0.0", 443)
        )

        async def request() -> str:
            with pytest.raises(ConnectionResetError) as caught:
                await asyncio.open_connection(
                    "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
                )
            return str(caught.value)

        message: str = await loop.net.host("client").create_task(request())
        server.close()
        await server.wait_closed()
        return message

    try:
        message = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert message == ""


def test_aborting_the_raw_transport_mid_handshake_resolves_the_connect() -> None:
    # Standard-library behaviour, not a simloop quirk: the waiter is woken
    # without an exception, so the connect returns the transport it would have
    # returned — already closed — and the application protocol, which never saw
    # connection_made, is never told the connection was lost either. Pinned so
    # nobody "fixes" it into an error later.
    loop = _network()
    events: list[str] = []

    class App(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            events.append("made")

        def connection_lost(self, exc: Exception | None) -> None:
            events.append(f"lost {exc!r}")

    class Hold(asyncio.Protocol):
        pass

    async def main() -> Any:
        running: Any = asyncio.get_running_loop()
        server = await loop.net.host("api").create_task(
            running.create_server(Hold, "0.0.0.0", 443)
        )

        async def request() -> Any:
            connecting = asyncio.ensure_future(
                running.create_connection(
                    App,
                    "api",
                    443,
                    ssl=_tls_certs.client_context(),
                    server_hostname="api",
                )
            )
            # After the syn/accept round trip, while the ClientHello is on the
            # wire and the handshake is still open.
            await asyncio.sleep(0.0025)
            for key, raw in list(loop.net._streams.items()):
                if key[1] == "client":
                    raw.abort()
            transport, _ = await connecting
            return transport

        transport = await loop.net.host("client").create_task(request())
        server.close()
        await server.wait_closed()
        return transport

    try:
        transport = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert transport.is_closing()
    assert events == []


def test_a_partition_stalls_the_handshake_until_it_heals() -> None:
    loop = _network()

    async def main() -> tuple[bool, bytes]:
        server = await _serve(loop)
        loop.net.partition({"api"}, {"client"})

        async def request() -> bytes:
            reader, writer = await asyncio.open_connection(
                "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
            )
            writer.write(b"hello")
            await writer.drain()
            reply = await reader.read(100)
            writer.close()
            await writer.wait_closed()
            return reply

        task = loop.net.host("client").create_task(request())
        await asyncio.sleep(3.0)
        stalled = not task.done()
        loop.net.heal()
        reply = await task
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return stalled, reply

    try:
        stalled, reply = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert stalled  # three virtual seconds in, still waiting and not failed
    assert reply == b"HELLO"


def test_a_partition_that_outlasts_the_deadline_aborts_the_handshake() -> None:
    loop = _network()

    class Hold(asyncio.Protocol):
        pass

    async def main() -> tuple[str, float]:
        running: Any = asyncio.get_running_loop()

        async def listen() -> Any:
            return await running.create_server(
                Hold, "0.0.0.0", 443, ssl=_tls_certs.server_context("api")
            )

        server = await loop.net.host("api").create_task(listen())
        # After the syn/accept round trip and before the ClientHello lands, so
        # the connection is established and only the handshake is cut off.
        loop.call_later(0.0025, loop.net.partition, {"api"}, {"client"})

        async def request() -> tuple[str, float]:
            started = running.time()
            with pytest.raises(ConnectionAbortedError) as caught:
                await asyncio.open_connection(
                    "api",
                    443,
                    ssl=_tls_certs.client_context(),
                    server_hostname="api",
                    ssl_handshake_timeout=5.0,
                )
            return str(caught.value), running.time() - started

        result: tuple[str, float] = await loop.net.host("client").create_task(
            request()
        )
        loop.net.heal()
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return result

    try:
        message, elapsed = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert "SSL handshake is taking longer than 5.0 seconds" in message
    assert 4.99 < elapsed < 5.1


def test_a_server_side_handshake_failure_leaves_the_run_green() -> None:
    loop = _network()
    made: list[Any] = []

    class App(asyncio.Protocol):
        def connection_made(self, transport: Any) -> None:
            made.append(transport)

    async def main() -> None:
        running: Any = asyncio.get_running_loop()

        async def listen() -> Any:
            return await running.create_server(
                App, "0.0.0.0", 443, ssl=_tls_certs.server_context("api")
            )

        server = await loop.net.host("api").create_task(listen())

        async def request() -> None:
            # The system trust store, which knows nothing of this simulation.
            with pytest.raises(ssl.SSLCertVerificationError):
                await asyncio.open_connection(
                    "api",
                    443,
                    ssl=ssl.create_default_context(),
                    server_hostname="api",
                )

        await loop.net.host("client").create_task(request())
        await asyncio.sleep(5.0)
        server.close()
        await server.wait_closed()

    try:
        loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert made == []  # the rejected client never reached the application


def _tls_run(seed: int) -> str:
    """Three concurrent TLS clients against one TLS server, hashed."""
    loop = SimLoop(seed=seed)
    for name in ("api", "one", "two", "three"):
        loop.net.host(name)
    loop.net.set_defaults(latency=(0.001, 0.02))

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write((await reader.read(100)).upper())
        await writer.drain()
        writer.close()

    async def request(name: str) -> bytes:
        reader, writer = await asyncio.open_connection(
            "api", 443, ssl=_tls_certs.client_context(), server_hostname="api"
        )
        writer.write(name.encode())
        await writer.drain()
        reply = await reader.read(100)
        writer.close()
        await writer.wait_closed()
        return reply

    async def main() -> list[bytes]:
        async def listen() -> Any:
            return await asyncio.start_server(
                handle, "0.0.0.0", 443, ssl=_tls_certs.server_context("api")
            )

        server = await loop.net.host("api").create_task(listen())
        tasks = [
            loop.net.host(name).create_task(request(name))
            for name in ("one", "two", "three")
        ]
        replies = [await task for task in tasks]
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return replies

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    return loop.trace_hash()


def test_the_same_seed_gives_the_same_tls_trace() -> None:
    for seed in (0, 3, 7):
        assert len({_tls_run(seed) for _ in range(5)}) == 1
    assert len({_tls_run(seed) for seed in (0, 3, 7)}) == 3


def test_ssl_rides_alongside_a_parked_socket() -> None:
    # The two-call sequence aiohttp performs: resolve, connect a real
    # descriptor, then hand it to create_connection with ssl= and
    # server_hostname= alongside.
    loop = _network()

    async def main() -> tuple[Any, Any]:
        server = await _serve(loop, handler=_hold_open)

        async def request() -> tuple[Any, Any]:
            running: Any = asyncio.get_running_loop()
            infos = await running.getaddrinfo("api", 443, type=socket.SOCK_STREAM)
            family, kind, proto, _, address = infos[0]
            sock = socket.socket(family=family, type=kind, proto=proto)
            sock.setblocking(False)
            await running.sock_connect(sock, address)
            transport, _ = await running.create_connection(
                asyncio.Protocol,
                ssl=_tls_certs.client_context(),
                server_hostname="api",
                sock=sock,
            )
            cipher = transport.get_extra_info("cipher")
            transport.close()
            return sock, cipher

        sock, cipher = await loop.net.host("client").create_task(request())
        await asyncio.sleep(2.0)
        server.close()
        await server.wait_closed()
        return sock, cipher

    try:
        sock, cipher = loop.run_until_complete(main())
        _settle(loop)
    finally:
        loop.close()
    assert cipher[1] == "TLSv1.3"
    assert sock.fileno() == -1  # the loop took ownership and closed it
    assert not loop._sock_targets  # the parked entry was claimed
