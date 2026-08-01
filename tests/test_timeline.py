"""The HTML timeline: lanes, arrows, marks, truncation and self-containment."""

from __future__ import annotations

import asyncio
import re

import pytest

from simloop import SimLoop, timeline_html
from simloop._trace import TraceEvent

# ----------------------------------------------------------------------
# A real two-host run whose invariant fails: beta sends three datagrams to
# alpha and only two arrive, because a partition lands while the third is in
# flight. The trace of that run is what most of these tests render.
# ----------------------------------------------------------------------


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[bytes] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append(data)


def _run_partitioned_exchange() -> tuple[SimLoop, list[bytes], list[bytes]]:
    loop = SimLoop(seed=0)
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    loop.net.set_defaults(latency=(0.05, 0.05))

    async def bind(port: int) -> tuple[asyncio.DatagramTransport, _Collector]:
        endpoint: tuple[asyncio.DatagramTransport, _Collector] = (
            await asyncio.get_running_loop().create_datagram_endpoint(
                _Collector, local_addr=("0.0.0.0", port)
            )
        )
        return endpoint

    async def main() -> tuple[list[bytes], list[bytes]]:
        receiver, at_alpha = await alpha.create_task(bind(7000))
        sender, at_beta = await beta.create_task(bind(7001))
        for number in range(2):
            sender.sendto(f"ping{number}".encode(), ("alpha", 7000))
            await asyncio.sleep(0.2)
        receiver.sendto(b"ack", ("beta", 7001))
        await asyncio.sleep(0.2)
        sender.sendto(b"ping2", ("alpha", 7000))
        await asyncio.sleep(0.01)  # still in flight: latency is 0.05
        loop.net.partition({"alpha"}, {"beta"})
        await asyncio.sleep(0.2)  # its delivery lands on the cut and is dropped
        loop.net.heal()
        loop.net.crash("beta")
        await asyncio.sleep(0.1)
        loop.net.restart("beta")
        await asyncio.sleep(0.1)
        receiver.close()
        await asyncio.sleep(0.01)
        return at_alpha.received, at_beta.received

    try:
        arrived, acked = loop.run_until_complete(main())
    finally:
        loop.close()
    return loop, arrived, acked


def _net_labels(loop: SimLoop) -> list[tuple[int, str]]:
    return [
        (event.seq, event.label.split(" ", 1)[0])
        for event in loop.trace
        if event.kind == "net"
    ]


def _elements(document: str, css_class: str) -> list[dict[str, str]]:
    """Every element opening tag with exactly this class, as attribute maps."""
    tags = re.findall(rf'<[a-z]+ class="{css_class}"[^>]*>', document)
    return [dict(re.findall(r'([\w-]+)="([^"]*)"', tag)) for tag in tags]


# ----------------------------------------------------------------------
# The page itself
# ----------------------------------------------------------------------


def test_a_failing_two_host_run_renders_one_svg_page() -> None:
    loop, arrived, _ = _run_partitioned_exchange()
    # The failure the timeline is there to explain: the third datagram never
    # arrived, because the partition landed while it was on the wire.
    assert arrived == [b"ping0", b"ping1"]
    document = timeline_html(loop.trace)
    assert document.startswith("<!doctype html>")
    assert document.count("<svg") == 1
    assert document.rstrip().endswith("</html>")


def test_the_page_fetches_nothing_from_anywhere() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    assert "http://" not in document
    assert "https://" not in document
    # Bare src=/href= only; the arrows carry data-src attributes, which are
    # not references to anything.
    assert not re.search(r"(?<![-\w])src=", document)
    assert not re.search(r"(?<![-\w])href=", document)
    assert "@import" not in document
    assert "xlink" not in document
    # Nothing fetchable is protocol-relative either. Asserted on the contexts
    # a browser would fetch from rather than on "//" anywhere in the file, so
    # that a comment in the script block can never fail this by accident.
    assert not re.search(r'(?:src|href)\s*=\s*"[^"]*//', document)
    assert not re.search(r"url\(\s*[^)]*//", document)
    # The one reference form the page does use points inside itself.
    assert re.findall(r"url\(.", document) == ["url(#"] * document.count("url(")


def test_a_lane_group_per_host_in_the_events() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    lanes = _elements(document, "lane")
    hosts = [lane["data-host"] for lane in lanes]
    # Every machine that ran or was named by a packet, plus one lane for the
    # simulation's own hostless work (clock advances, wire steps).
    assert set(hosts) == {"alpha", "beta", "driver", ""}
    assert len(hosts) == len(set(hosts))
    assert '<text class="lane-name"' in document


def test_one_arrow_per_delivered_packet() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    delivered = sorted(uid for uid, verb in _net_labels(loop) if verb == "deliver")
    arrows = _elements(document, "arrow")
    assert len(delivered) >= 3
    assert sorted(int(arrow["data-uid"]) for arrow in arrows) == delivered
    both_ways = {(arrow["data-src"], arrow["data-dst"]) for arrow in arrows}
    assert both_ways == {("beta", "alpha"), ("alpha", "beta")}
    for arrow in arrows:
        assert float(arrow["data-end"]) > float(arrow["data-start"])


def test_a_dropped_datagram_leaves_a_stub() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    dropped = [uid for uid, verb in _net_labels(loop) if verb == "drop"]
    assert len(dropped) == 1
    stubs = _elements(document, "stub")
    assert [int(stub["data-uid"]) for stub in stubs] == dropped
    assert stubs[0]["data-fate"] == "drop"
    assert stubs[0]["data-src"] == "beta"


def test_crash_and_restart_mark_the_lane() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    marks = _elements(document, "mark")
    by_verb = {mark["data-mark"]: mark for mark in marks}
    # The loud lane mark is reserved for a machine's fate: nothing that merely
    # happened to a packet is allowed to look like a crash.
    assert set(by_verb) == {"crash", "restart"}
    assert by_verb["crash"]["data-host"] == "beta"
    assert by_verb["restart"]["data-host"] == "beta"
    assert float(by_verb["restart"]["data-when"]) > float(by_verb["crash"]["data-when"])


def test_scheduling_events_become_dots_on_their_host_lane() -> None:
    loop, _, _ = _run_partitioned_exchange()
    document = timeline_html(loop.trace)
    dots = _elements(document, "dot")
    scheduling = [event for event in loop.trace if event.kind != "net"]
    assert len(dots) == len(scheduling)
    assert {"schedule", "run", "advance"} <= {dot["data-kind"] for dot in dots}
    # Hover text, which is the only way to read a dot's label.
    assert "<title>" in document


# ----------------------------------------------------------------------
# Packet shapes the network really produces, pinned synthetically so the
# pairing rules are readable next to the assertion.
# ----------------------------------------------------------------------


def _net(uid: int, when: float, label: str) -> TraceEvent:
    return TraceEvent("net", when, uid, label)


def test_a_held_packet_pairs_its_arrow_to_the_second_send() -> None:
    # The shape test_trace.py pins: a packet in flight when the cut lands is
    # sent, held, released and sent again, and is delivered once. The arrow
    # belongs to the send that actually crossed.
    events = [
        _net(1, 0.0, "send a>b"),
        _net(1, 0.05, "hold a>b"),
        _net(1, 0.5, "release a>b"),
        _net(1, 0.5, "send a>b"),
        _net(1, 0.55, "deliver a>b"),
    ]
    document = timeline_html(events)
    arrows = _elements(document, "arrow")
    assert len(arrows) == 1
    assert float(arrows[0]["data-start"]) == pytest.approx(0.5)
    assert float(arrows[0]["data-end"]) == pytest.approx(0.55)
    # The first send did not reach anyone, and says so.
    stubs = _elements(document, "stub")
    assert [stub["data-fate"] for stub in stubs] == ["hold"]
    assert float(stubs[0]["data-start"]) == pytest.approx(0.0)


def test_a_packet_held_before_it_was_ever_sent_marks_the_lane() -> None:
    # The other shape: written while the cut stood, so it never left. There is
    # no send to draw a stub from, and the hold still has to be visible.
    events = [
        _net(2, 0.0, "hold a>b"),
        _net(2, 0.5, "release a>b"),
        _net(2, 0.5, "send a>b"),
        _net(2, 0.55, "deliver a>b"),
    ]
    document = timeline_html(events)
    assert len(_elements(document, "arrow")) == 1
    assert not _elements(document, "stub")
    fates = _elements(document, "fate")
    assert [fate["data-mark"] for fate in fates] == ["hold", "release"]
    # Held at the sending end, so it is the sender's lane that says so.
    assert fates[0]["data-host"] == "a"
    assert not _elements(document, "mark")


def test_a_packets_fate_is_marked_apart_from_a_machines() -> None:
    # A chaos run drops thousands of datagrams and crashes a handful of
    # machines. If both drew the same tick, the crashes would be invisible.
    events = [
        _net(5, 0.0, "drop a>b"),
        _net(6, 0.1, "crash b"),
    ]
    document = timeline_html(events)
    fate = _elements(document, "fate")[0]
    mark = _elements(document, "mark")[0]
    assert fate["data-mark"] == "drop"
    assert mark["data-mark"] == "crash"

    def height(tick: dict[str, str]) -> float:
        return abs(float(tick["y2"]) - float(tick["y1"]))

    assert height(fate) < height(mark)
    assert "packet dropped" in document and "machine crashed" in document


def test_a_loss_marks_the_lane_the_packet_was_lost_on() -> None:
    # A datagram that arrives at a live machine with nothing bound to take it
    # is recorded as delivered and then lost. The loss happened where it
    # landed, not where it was sent from.
    events = [
        _net(7, 0.0, "send a>b"),
        _net(7, 0.1, "deliver a>b"),
        _net(7, 0.1, "lost a>b"),
    ]
    document = timeline_html(events)
    assert len(_elements(document, "arrow")) == 1
    fates = _elements(document, "fate")
    assert [(fate["data-mark"], fate["data-host"]) for fate in fates] == [("lost", "b")]


def test_a_net_label_naming_no_machine_lands_in_the_simulation_lane() -> None:
    # Nothing records this today; a verb added later must still draw rather
    # than vanish.
    document = timeline_html([_net(8, 0.0, "quiesce")])
    assert '<g class="lane" data-host=""' in document
    fates = _elements(document, "fate")
    assert [(fate["data-mark"], fate["data-host"]) for fate in fates] == [
        ("quiesce", "")
    ]


def test_a_duplicated_datagram_draws_both_arrows() -> None:
    # A duplicate is one uid sent twice and delivered twice; each delivery is
    # paired with its own send, oldest first.
    events = [
        _net(3, 0.0, "dup a>b"),
        _net(3, 0.0, "send a>b"),
        _net(3, 0.0, "send a>b"),
        _net(3, 0.1, "deliver a>b"),
        _net(3, 0.2, "deliver a>b"),
    ]
    arrows = _elements(timeline_html(events), "arrow")
    assert [float(arrow["data-end"]) for arrow in arrows] == [
        pytest.approx(0.1),
        pytest.approx(0.2),
    ]


def test_a_packet_still_in_flight_when_the_run_ended_is_a_stub() -> None:
    events = [_net(4, 0.0, "send a>b")]
    stubs = _elements(timeline_html(events), "stub")
    assert [stub["data-fate"] for stub in stubs] == ["inflight"]


# ----------------------------------------------------------------------
# Size guardrail
# ----------------------------------------------------------------------


def _synthetic(count: int) -> list[TraceEvent]:
    return [
        TraceEvent("run", index / 100, index, f"step_{index}", "alpha")
        for index in range(count)
    ]


def test_the_default_limit_keeps_the_last_events_and_says_so() -> None:
    document = timeline_html(_synthetic(6000))
    assert 'data-events="5000"' in document
    assert 'data-total="6000"' in document
    assert "showing the last 5,000 of 6,000 events" in document
    # Hover text ends with the label, so this pins which end was kept.
    assert "step_5999<" in document
    assert "step_999<" not in document


def test_no_banner_when_nothing_was_dropped() -> None:
    document = timeline_html(_synthetic(10))
    assert 'data-events="10"' in document
    assert "showing the last" not in document


def test_the_limit_can_be_lifted() -> None:
    document = timeline_html(_synthetic(6000), limit=None)
    assert 'data-events="6000"' in document
    assert "showing the last" not in document
    assert "step_0<" in document


def test_a_limit_below_one_is_refused() -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        timeline_html(_synthetic(10), limit=0)


# ----------------------------------------------------------------------
# Escaping and empty input
# ----------------------------------------------------------------------


def test_labels_and_hosts_are_escaped() -> None:
    events = [
        TraceEvent("run", 0.0, 0, "<script>alert('x')</script>", "<b>&"),
    ]
    document = timeline_html(events)
    assert "<script>alert" not in document
    assert "&lt;script&gt;alert(&#x27;x&#x27;)&lt;/script&gt;" in document
    assert "<b>&" not in document
    assert "&lt;b&gt;&amp;" in document


def test_an_empty_trace_still_renders_a_page() -> None:
    document = timeline_html([])
    assert document.startswith("<!doctype html>")
    assert 'data-events="0"' in document
    assert "no events" in document
