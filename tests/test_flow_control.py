"""Simulated write-side flow control: accounting, watermarks, and the switch."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from simloop import SimLoop, SimulationDeadlockError
from simloop._explore import explore
from simloop._run import finish


def _network(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    return loop


class _Sink(asyncio.Protocol):
    """Server end that records what it was given and can refuse to read."""

    pause_on_connect = False

    def __init__(self) -> None:
        self.transport: Any = None
        self.data = bytearray()
        self.eof = False
        self.lost: list[BaseException | None] = []

    def connection_made(self, transport: Any) -> None:
        self.transport = transport
        if self.pause_on_connect:
            transport.pause_reading()

    def data_received(self, data: bytes) -> None:
        self.data += data

    def eof_received(self) -> bool:
        self.eof = True
        return True

    def connection_lost(self, exc: BaseException | None) -> None:
        self.lost.append(exc)


class _HeldSink(_Sink):
    pause_on_connect = True


class _WatchedStreams(asyncio.StreamReaderProtocol):
    """A stream protocol that notes the teardown its transport delivered."""

    def __init__(
        self, reader: asyncio.StreamReader, lost: list[Exception | None]
    ) -> None:
        super().__init__(reader)
        self._lost = lost

    def connection_lost(self, exc: Exception | None) -> None:
        self._lost.append(exc)
        super().connection_lost(exc)


class _Writer(asyncio.Protocol):
    """Client end that notes the flow-control callbacks it was sent."""

    def __init__(self) -> None:
        self.transport: Any = None
        self.events: list[str] = []
        self.data = bytearray()

    def connection_made(self, transport: Any) -> None:
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        self.data += data

    def pause_writing(self) -> None:
        self.events.append("pause")

    def resume_writing(self) -> None:
        self.events.append("resume")


async def _serve(factory: Any, port: int = 9000) -> None:
    async def start() -> None:
        await asyncio.get_running_loop().create_server(factory, "0.0.0.0", port)

    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    await loop.net.host("server").create_task(start())


async def _pair(port: int = 9000) -> tuple[_HeldSink, Any, _Writer]:
    """A server that will not read and a client protocol pinned to its own host."""
    sinks: list[_HeldSink] = []

    def server_factory() -> _HeldSink:
        sink = _HeldSink()
        sinks.append(sink)
        return sink

    await _serve(server_factory, port)
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)

    async def connect() -> tuple[Any, _Writer]:
        transport, protocol = await asyncio.get_running_loop().create_connection(
            _Writer, "server", port
        )
        return transport, protocol

    transport, client = await loop.net.host("client").create_task(connect())
    return sinks[0], transport, client


def test_a_run_that_asks_for_nothing_reports_no_write_buffer() -> None:
    loop = _network()

    async def main() -> tuple[int, list[str]]:
        sink, transport, client = await _pair()
        for _ in range(64):
            transport.write(b"x" * 1024)
        size = transport.get_write_buffer_size()
        await asyncio.sleep(0.01)
        return size, client.events

    try:
        size, events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert size == 0
    assert events == []


def test_watermark_crossing_pauses_and_resumes_exactly_once() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> list[str]:
        sink, transport, client = await _pair()
        for _ in range(5):
            transport.write(b"x" * 1024)
        assert client.events == ["pause"]
        await asyncio.sleep(0.01)
        sink.transport.resume_reading()
        return client.events

    try:
        events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert events == ["pause", "resume"]


def test_the_buffer_counts_bytes_the_peer_has_not_received() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[int, int, int]:
        sink, transport, client = await _pair()
        for _ in range(5):
            transport.write(b"x" * 1024)
        written = transport.get_write_buffer_size()
        await asyncio.sleep(0.01)
        in_flight = transport.get_write_buffer_size()  # still owed: peer is paused
        sink.transport.resume_reading()
        return written, in_flight, transport.get_write_buffer_size()

    try:
        written, in_flight, drained = loop.run_until_complete(main())
    finally:
        loop.close()
    assert written == 5 * 1024
    assert in_flight == 5 * 1024
    assert drained == 0


def test_drain_blocks_until_the_peer_reads() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[float, float]:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)
        running = asyncio.get_running_loop()

        async def request() -> float:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            await writer.drain()
            return running.time()

        task = loop.net.host("client").create_task(request())
        await asyncio.sleep(0.01)
        running.call_later(1.0, sinks[0].transport.resume_reading)
        expected = running.time() + 1.0
        return await task, expected

    try:
        released, expected = loop.run_until_complete(main())
    finally:
        loop.close()
    # The drain returns in the very step the peer's read credited it back.
    assert released == pytest.approx(expected)


def test_writes_are_allowed_while_paused_and_grow_the_buffer() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[int, list[str]]:
        sink, transport, client = await _pair()
        for _ in range(5):
            transport.write(b"x" * 1024)
        assert client.events == ["pause"]
        for _ in range(5):
            transport.write(b"x" * 1024)
        return transport.get_write_buffer_size(), client.events

    try:
        size, events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert size == 10 * 1024
    assert events == ["pause"]


def test_writelines_crosses_the_watermark_once() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[int, list[str]]:
        sink, transport, client = await _pair()
        transport.writelines([b"x" * 1024] * 5)
        return transport.get_write_buffer_size(), client.events

    try:
        size, events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert size == 5 * 1024
    assert events == ["pause"]


def test_limits_set_on_a_transport_override_the_network_default() -> None:
    loop = _network()
    loop.net.set_flow_control()

    async def main() -> tuple[tuple[int, int], tuple[int, int], list[str]]:
        sink, transport, client = await _pair()
        network_default = transport.get_write_buffer_limits()
        transport.set_write_buffer_limits(high=2048, low=512)
        own = transport.get_write_buffer_limits()
        for _ in range(5):
            transport.write(b"x" * 1024)
        return network_default, own, client.events

    try:
        network_default, own, events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert network_default == (16 * 1024, 64 * 1024)
    assert own == (512, 2048)
    # 5 KiB is under the network default and over this transport's own mark.
    assert events == ["pause"]


def test_lowering_the_limits_pauses_immediately() -> None:
    loop = _network()
    loop.net.set_flow_control()

    async def main() -> tuple[list[str], list[str]]:
        sink, transport, client = await _pair()
        for _ in range(5):
            transport.write(b"x" * 1024)
        under_default = list(client.events)
        transport.set_write_buffer_limits(high=1024, low=256)
        return under_default, client.events

    try:
        under_default, events = loop.run_until_complete(main())
    finally:
        loop.close()
    assert under_default == []
    assert events == ["pause"]


def test_bad_limits_are_rejected() -> None:
    loop = _network()

    async def main() -> None:
        sink, transport, client = await _pair()
        with pytest.raises(ValueError):
            transport.set_write_buffer_limits(high=1, low=2)

    try:
        with pytest.raises(ValueError):
            loop.net.set_flow_control(high=1, low=2)
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_turning_flow_control_off_releases_a_paused_writer() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[float, float, list[str]]:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)
        running = asyncio.get_running_loop()
        seen: list[str] = []

        async def request() -> float:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            seen.append("writing")
            await writer.drain()
            seen.append("drained")
            assert writer.transport.get_write_buffer_size() == 0
            return running.time()

        task = loop.net.host("client").create_task(request())
        await asyncio.sleep(0.01)
        assert seen == ["writing"]
        running.call_later(1.0, lambda: loop.net.set_flow_control(enabled=False))
        expected = running.time() + 1.0
        return await task, expected, seen

    try:
        released, expected, seen = loop.run_until_complete(main())
    finally:
        loop.close()
    assert released == pytest.approx(expected)
    assert seen == ["writing", "drained"]


def test_datagram_transports_have_no_write_buffer() -> None:
    # Write-side flow control is a stream feature: a datagram endpoint queues
    # nothing on anyone's behalf, so it does not answer for a write buffer.
    loop = _network()

    async def main() -> Any:
        transport, _protocol = await asyncio.get_running_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("0.0.0.0", 7000)
        )
        return transport

    try:
        transport = loop.run_until_complete(main())
    finally:
        loop.close()
    assert not hasattr(transport, "get_write_buffer_size")
    assert not hasattr(transport, "set_write_buffer_limits")


def test_write_eof_while_paused_still_reaches_the_peer() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[list[str], int, bool]:
        sink, transport, client = await _pair()
        for _ in range(5):
            transport.write(b"x" * 1024)
        transport.write_eof()
        assert client.events == ["pause"]
        await asyncio.sleep(0.01)
        assert not sink.eof
        sink.transport.resume_reading()
        return client.events, len(sink.data), sink.eof

    try:
        events, received, eof = loop.run_until_complete(main())
    finally:
        loop.close()
    assert events == ["pause", "resume"]
    assert received == 5 * 1024
    assert eof is True


def test_close_while_a_drain_waits_wakes_it() -> None:
    # FlowControlMixin.connection_lost(None) resolves every waiter, because the
    # protocol is paused when the close lands.
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> Any:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)

        async def request() -> Any:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            drain = asyncio.ensure_future(writer.drain())
            await asyncio.sleep(0.01)
            assert not drain.done()
            writer.transport.close()
            return await drain

        return await loop.net.host("client").create_task(request())

    try:
        outcome = loop.run_until_complete(main())
    finally:
        loop.close()
    assert outcome is None


def test_drain_started_after_close_raises_connection_reset() -> None:
    # The other stdlib branch: drain() on a transport whose connection_lost has
    # already run reports the loss instead of waiting. Which branch a drain
    # started in the same step as close() takes is a scheduling decision, so
    # this waits for the teardown to land rather than racing it.
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> None:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)

        async def request() -> None:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            writer.transport.close()
            await asyncio.sleep(0.01)
            with pytest.raises(ConnectionResetError):
                await writer.drain()

        await loop.net.host("client").create_task(request())

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_a_peer_reset_fails_a_pending_drain() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> None:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)

        async def request() -> None:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            with pytest.raises(ConnectionResetError):
                await writer.drain()

        task = loop.net.host("client").create_task(request())
        await asyncio.sleep(0.01)
        sinks[0].transport.abort()
        await task

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_a_partition_applies_backpressure_and_a_heal_lifts_it() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[float, int]:
        sinks: list[_Sink] = []

        def server_factory() -> _Sink:
            sink = _Sink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)
        running = asyncio.get_running_loop()

        async def request() -> float:
            _reader, writer = await asyncio.open_connection("server", 9000)
            loop.net.partition({"server"}, {"client"})
            writer.write(b"x" * 5120)
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5):
                    await writer.drain()
            running.call_later(0.5, loop.net.heal)
            await writer.drain()
            return running.time()

        released = await loop.net.host("client").create_task(request())
        return released, len(sinks[0].data)

    try:
        released, received = loop.run_until_complete(main())
    finally:
        loop.close()
    assert released == pytest.approx(1.0)
    assert received == 5120


def test_a_crashed_peer_leaves_the_writer_paused_until_its_own_timeout() -> None:
    # A crashed host sends no reset, so the survivor's transport is untouched
    # by design and only its own clock ever tells it anything.
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> None:
        sinks: list[_Sink] = []

        def server_factory() -> _Sink:
            sink = _Sink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)

        async def request() -> None:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"x" * 5120)
            loop.net.crash("server")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(1.0):
                    await writer.drain()

        await loop.net.host("client").create_task(request())

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()


def test_a_crashing_writer_releases_its_own_drain() -> None:
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def main() -> tuple[bool, list[Exception | None]]:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)
        lost: list[Exception | None] = []

        async def request() -> None:
            # open_connection's own assembly, so the stream protocol can be
            # watched: everything else about the connection is unchanged.
            running = asyncio.get_running_loop()
            reader = asyncio.StreamReader(loop=running)
            protocol = _WatchedStreams(reader, lost)
            transport, _ = await running.create_connection(
                lambda: protocol, "server", 9000
            )
            writer = asyncio.StreamWriter(transport, protocol, reader, running)
            writer.write(b"x" * 5120)
            await writer.drain()

        task = loop.net.host("client").create_task(request())
        await asyncio.sleep(0.01)
        assert not task.done()
        loop.net.crash("client")
        await asyncio.sleep(0.01)
        return task.done(), lost

    try:
        done, lost = loop.run_until_complete(main())
    finally:
        loop.close()
    assert done is True
    assert lost == [None]


def _crossing(seed: int, armed: bool) -> str:
    """A run whose writer really crosses a watermark, traced end to end."""
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    loop.net.set_defaults(latency=(0.001, 0.02))
    if armed:
        loop.net.set_flow_control(high=2048, low=512)

    async def main() -> None:
        sinks: list[_HeldSink] = []

        def server_factory() -> _HeldSink:
            sink = _HeldSink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)
        running = asyncio.get_running_loop()

        async def request() -> None:
            _reader, writer = await asyncio.open_connection("server", 9000)
            for _ in range(4):
                writer.write(b"x" * 4096)
                await writer.drain()

        task = loop.net.host("client").create_task(request())
        for _ in range(8):
            await asyncio.sleep(0.05)
            if sinks[0].transport.is_reading():
                sinks[0].transport.pause_reading()
            else:
                sinks[0].transport.resume_reading()
        await task
        assert running.time() > 0.0

    try:
        loop.run_until_complete(main())
        return loop.trace_hash()
    finally:
        loop.close()


def test_the_same_seed_traces_identically_under_flow_control() -> None:
    assert _crossing(3, armed=True) == _crossing(3, armed=True)
    # A crossing is visible in the trace: the writer's wakeup is scheduled by
    # the read that released it, so an armed run cannot hash like an idle one.
    assert _crossing(3, armed=True) != _crossing(3, armed=False)


def _small_exchange(armed: bool) -> str:
    loop = SimLoop(seed=1)
    loop.net.host("server")
    loop.net.host("client")
    if armed:
        loop.net.set_flow_control()

    async def main() -> None:
        sinks: list[_Sink] = []

        def server_factory() -> _Sink:
            sink = _Sink()
            sinks.append(sink)
            return sink

        await _serve(server_factory)

        async def request() -> None:
            _reader, writer = await asyncio.open_connection("server", 9000)
            writer.write(b"hello")
            await writer.drain()
            await asyncio.sleep(0.1)
            writer.close()

        await loop.net.host("client").create_task(request())
        await asyncio.sleep(0.01)
        assert bytes(sinks[0].data) == b"hello"

    try:
        loop.run_until_complete(main())
        return loop.trace_hash()
    finally:
        loop.close()


def test_a_workload_that_never_crosses_a_watermark_traces_the_same_either_way() -> None:
    assert _small_exchange(False) == _small_exchange(True)


# Both ends answer before they consume, so neither ever credits the other.
# Sizes are chosen against the stream readers' own 1 KiB buffers: the first
# few chunks are taken and credited, the reader then stops reading, and what
# is left unread is more than the 2 KiB high mark.
_HEADER = b"h" * 32 + b"\n"
_CHUNK = b"x" * 1024
_CHUNKS = 8
_READER_LIMIT = 1024


def test_a_mutual_drain_is_a_deadlock() -> None:
    # No timer is pending while both ends sit in drain(), so the run can never
    # make progress — the backpressure form of the missing-timeout bug. The
    # stall leaves the server's handler task pending, so teardown goes through
    # finish(), which cancels it instead of letting its collection print.
    loop = _network()
    loop.net.set_flow_control(high=2048, low=512)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        for _ in range(_CHUNKS):
            writer.write(_CHUNK)
        await writer.drain()
        await reader.readexactly(_CHUNKS * len(_CHUNK))

    async def main() -> None:
        async def start() -> None:
            await asyncio.start_server(handle, "0.0.0.0", 9000, limit=_READER_LIMIT)

        await loop.net.host("server").create_task(start())
        reader, writer = await asyncio.open_connection(
            "server", 9000, limit=_READER_LIMIT
        )
        writer.write(_HEADER)
        for _ in range(_CHUNKS):
            writer.write(_CHUNK)
        await writer.drain()
        await reader.readexactly(_CHUNKS * len(_CHUNK))

    try:
        with pytest.raises(SimulationDeadlockError):
            loop.run_until_complete(main())
    finally:
        finish(loop)


# The same shape as the deadlock above, but the server answers after a short
# read window instead of after a fixed number of bytes, and every run is
# bounded so it terminates either way. How much of the body the server took
# before its own response filled its buffer then depends on the latency draws:
# take enough and the credit lifts the client's pause, take too little and
# both ends sit in drain() with neither reading. The sizes sit near the
# watermark on purpose — far above it every seed hangs, far below it none do,
# and neither teaches anything about the seed that matters.
_BODY_CHUNK = 1536
_BODY_CHUNKS = 8
_STREAM_LIMIT = 1024
_READ_WINDOW = 0.012
_RUN_BUDGET = 5.0
# Observed by running the search below; seeds 0 and 1 finish, seed 2 does not.
_FOUND_SEED = 2


async def _answer_before_reading() -> None:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, SimLoop)
    net = loop.net
    net.host("server")
    net.host("client")
    net.set_defaults(latency=(0.001, 0.02))
    net.set_flow_control(high=2048, low=512)

    total = _BODY_CHUNK * _BODY_CHUNKS
    header = b"h" * 32 + b"\n"
    body = b"x" * _BODY_CHUNK

    async def read_up_to(reader: asyncio.StreamReader, want: int) -> int:
        # Incremental, so the reader's own buffer limit is never what stalls a
        # run: anything waiting here is waiting on the write side.
        got = 0
        while got < want:
            piece = await reader.read(_BODY_CHUNK)
            if not piece:
                break
            got += len(piece)
        return got

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        # Counted as it goes: what the window cut short still has to be
        # subtracted from what is left to read after the response.
        taken = [0]
        try:
            async with asyncio.timeout(_READ_WINDOW):
                while taken[0] < total:
                    piece = await reader.read(_BODY_CHUNK)
                    if not piece:
                        break
                    taken[0] += len(piece)
        except TimeoutError:
            pass
        consumed = taken[0]
        for _ in range(_BODY_CHUNKS):
            writer.write(body)
        await writer.drain()
        await read_up_to(reader, total - consumed)

    async def start() -> None:
        await asyncio.start_server(
            handle, "0.0.0.0", 9000, limit=_STREAM_LIMIT
        )

    await net.host("server").create_task(start())

    async def request() -> None:
        reader, writer = await asyncio.open_connection(
            "server", 9000, limit=_STREAM_LIMIT
        )
        writer.write(header)
        for _ in range(_BODY_CHUNKS):
            writer.write(body)
        await writer.drain()
        await read_up_to(reader, total)

    async with asyncio.timeout(_RUN_BUDGET):
        await net.host("client").create_task(request())


def test_a_seed_finds_the_backpressure_deadlock() -> None:
    report = explore(_answer_before_reading, range(32))
    assert report is not None
    assert report.seed == _FOUND_SEED
    assert report.seeds_passed == _FOUND_SEED
    assert isinstance(report.exception, TimeoutError)
