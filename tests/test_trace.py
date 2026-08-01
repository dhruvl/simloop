"""The trace record: ordering, host attribution and hash injectivity."""

from __future__ import annotations

import asyncio

import pytest

from simloop import SimLoop
from simloop._trace import TraceEvent, TraceRecorder


def test_events_are_recorded_in_order() -> None:
    recorder = TraceRecorder()
    recorder.record("schedule", 0.0, 0, "f")
    recorder.record("run", 0.0, 0, "f")
    assert [event.kind for event in recorder.events] == ["schedule", "run"]
    assert recorder.events[0].seq == 0
    assert recorder.events[0].label == "f"


def test_identical_sequences_hash_identically() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    for recorder in (first, second):
        recorder.record("schedule", 0.0, 0, "f")
        recorder.record("advance", 1.5, -1, "")
        recorder.record("run", 1.5, 0, "f")
    assert first.hash() == second.hash()
    assert len(first.hash()) == 64


def test_different_order_hashes_differently() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "f")
    first.record("run", 0.0, 1, "g")
    second.record("run", 0.0, 1, "g")
    second.record("run", 0.0, 0, "f")
    assert first.hash() != second.hash()


def test_cancel_events_change_the_hash() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "f")
    second.record("run", 0.0, 0, "f")
    second.record("cancel", 0.0, 1, "g")
    assert first.hash() != second.hash()


def test_events_are_immutable_and_hostless_by_default() -> None:
    event = TraceEvent("run", 0.0, 0, "f")
    assert event.host == ""
    assert TraceEvent._fields == ("kind", "when", "seq", "label", "host")
    assert not hasattr(event, "__dict__")
    with pytest.raises(AttributeError):
        event.host = "alpha"  # type: ignore[misc]


def test_the_host_field_is_part_of_the_hash() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "f", host="alpha")
    second.record("run", 0.0, 0, "f", host="beta")
    assert first.hash() != second.hash()
    assert first.events[0].host == "alpha"


def test_the_host_field_is_delimited_from_the_label() -> None:
    # Without a separator between the two fields, "ab" + "" and "a" + "b"
    # would serialize to the same bytes; the delimiter is what keeps the
    # serialization injective.
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "", host="ab")
    second.record("run", 0.0, 0, "b", host="a")
    assert first.hash() != second.hash()


# ----------------------------------------------------------------------
# Attribution and delivery under a real run
# ----------------------------------------------------------------------


async def _echo_once(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write((await reader.readline()).upper())
    await writer.drain()
    writer.close()
    await writer.wait_closed()


def _run_stream_exchange(seed: int = 0) -> SimLoop:
    loop = SimLoop(seed=seed)
    loop.net.host("server")
    loop.net.host("client")
    loop.net.set_defaults(latency=(0.01, 0.02))

    async def serve() -> None:
        server = await asyncio.start_server(_echo_once, "0.0.0.0", 9000)
        async with server:
            await asyncio.sleep(10.0)

    async def request() -> bytes:
        reader, writer = await asyncio.open_connection("server", 9000)
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
        # Let the teardown packets land: a packet still in flight when the run
        # ends has been sent and not yet delivered, which is honest but says
        # nothing about whether deliveries are recorded.
        await asyncio.sleep(0.5)
        return reply

    try:
        assert loop.run_until_complete(main()) == b"PING\n"
    finally:
        loop.close()
    return loop


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[bytes] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append(data)


def _run_datagram_exchange(seed: int, *, drop: float, count: int) -> SimLoop:
    loop = SimLoop(seed=seed)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    loop.net.set_defaults(latency=(0.01, 0.02))
    loop.net.set_link("beta", "alpha", drop=drop)

    async def bind(port: int) -> tuple[asyncio.DatagramTransport, _Collector]:
        endpoint: tuple[asyncio.DatagramTransport, _Collector] = (
            await asyncio.get_running_loop().create_datagram_endpoint(
                _Collector, local_addr=("0.0.0.0", port)
            )
        )
        return endpoint

    async def main() -> None:
        receiver, _ = await alpha.create_task(bind(7000))
        sender, _ = await beta.create_task(bind(7001))

        async def send_all() -> None:
            for number in range(count):
                sender.sendto(f"ping{number}".encode(), ("alpha", 7000))
                await asyncio.sleep(0.05)

        await beta.create_task(send_all())
        await asyncio.sleep(1.0)
        receiver.close()
        sender.close()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    return loop


def _verbs_by_uid(loop: SimLoop) -> dict[int, list[str]]:
    verbs: dict[int, list[str]] = {}
    for event in loop.trace:
        if event.kind == "net":
            verbs.setdefault(event.seq, []).append(event.label.split(" ", 1)[0])
    return verbs


def test_run_events_are_attributed_to_the_executing_host() -> None:
    loop = _run_stream_exchange()
    run_hosts = {event.host for event in loop.trace if event.kind == "run"}
    assert {"client", "server"} <= run_hosts


def test_network_and_clock_events_carry_no_host() -> None:
    # A packet event belongs to a link, not a machine: its label already
    # names both ends, and a clock advance belongs to the whole simulation.
    loop = _run_stream_exchange()
    assert [event for event in loop.trace if event.kind == "net"]
    assert all(
        event.host == "" for event in loop.trace if event.kind in ("net", "advance")
    )


def test_delivery_steps_belong_to_the_wire_not_the_sender() -> None:
    # Crossing the wire is the simulation's work: the sender asked for it (so
    # the schedule event names it), but no machine's code is what runs.
    loop = _run_stream_exchange()
    deliveries = [event for event in loop.trace if event.label == "SimNetwork._deliver"]
    runs = [event for event in deliveries if event.kind in ("run", "cancel")]
    schedules = [event for event in deliveries if event.kind == "schedule"]
    assert runs and schedules
    assert {event.host for event in runs} == {""}
    assert {"client", "server"} <= {event.host for event in schedules}


def test_scheduling_events_name_the_host_that_scheduled_them() -> None:
    loop = _run_stream_exchange()
    schedule_hosts = {event.host for event in loop.trace if event.kind == "schedule"}
    assert {"client", "server", "driver"} <= schedule_hosts


def test_every_arriving_packet_is_recorded_as_delivered() -> None:
    loop = _run_stream_exchange()
    labels = [event.label for event in loop.trace if event.kind == "net"]
    assert "deliver client>server" in labels
    assert "deliver server>client" in labels
    verbs = _verbs_by_uid(loop)
    assert verbs
    for uid, seen in verbs.items():
        if "send" not in seen:
            continue
        assert seen.count("deliver") == 1, (uid, seen)


def test_held_stream_packets_are_sent_again_and_delivered_once() -> None:
    # The two shapes a partition produces, and the reason a reader cannot
    # assume one "send" per uid: a packet already in flight when the cut lands
    # is sent, held on arrival, then sent a second time after the heal; a
    # packet written while the cut stands never leaves, so it is held with no
    # "send" at all. Both end in exactly one "deliver".
    loop = SimLoop(seed=0)
    server = loop.net.host("server")
    client = loop.net.host("client")
    loop.net.set_defaults(latency=(0.05, 0.05))
    lines: list[bytes] = []

    async def collect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while line := await reader.readline():
            lines.append(line)
        writer.close()

    async def serve() -> None:
        listener = await asyncio.start_server(collect, "0.0.0.0", 9000)
        async with listener:
            await asyncio.sleep(10.0)

    async def connect() -> asyncio.StreamWriter:
        _, writer = await asyncio.open_connection("server", 9000)
        return writer

    async def main() -> None:
        serve_task = server.create_task(serve())
        await asyncio.sleep(0.01)
        # Writes carry the transport's own host as their source, so the driver
        # can drive both ends of the exchange from here.
        writer: asyncio.StreamWriter = await client.create_task(connect())
        writer.write(b"first\n")
        await asyncio.sleep(0.01)  # still in flight: latency is 0.05
        loop.net.partition({"client"}, {"server"})
        await asyncio.sleep(0.2)  # its delivery lands on the cut and is held
        writer.write(b"second\n")  # never leaves: held at transmission
        await asyncio.sleep(0.2)
        loop.net.heal()
        await asyncio.sleep(0.5)
        writer.close()
        await asyncio.sleep(0.3)
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.5)

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

    assert lines == [b"first\n", b"second\n"]
    verbs = _verbs_by_uid(loop)
    held = [seen for seen in verbs.values() if "hold" in seen]
    assert held == [
        ["send", "hold", "release", "send", "deliver"],
        ["hold", "release", "send", "deliver"],
    ]
    for uid, seen in verbs.items():
        assert seen.count("deliver") <= 1, (uid, seen)


def test_dropped_datagrams_are_never_delivered() -> None:
    loop = _run_datagram_exchange(seed=0, drop=0.5, count=12)
    verbs = _verbs_by_uid(loop)
    dropped = [uid for uid, seen in verbs.items() if "drop" in seen]
    delivered = [uid for uid, seen in verbs.items() if "deliver" in seen]
    assert dropped and delivered
    for uid in dropped:
        assert "send" not in verbs[uid]
        assert "deliver" not in verbs[uid]
    for uid, seen in verbs.items():
        if "send" not in seen or {"drop", "lost", "hold", "dup"} & set(seen):
            continue
        assert seen.count("deliver") == 1, (uid, seen)
