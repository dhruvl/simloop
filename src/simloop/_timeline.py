"""Draw a recorded trace as a host timeline, as one self-contained page.

The page is a single HTML file with its CSS, its script and its SVG inline:
it opens from disk, from a CI artifact store or from an attachment, and it
never asks the network for anything. That is a deliberate constraint — a
failure artifact that needs a CDN to render is not an artifact — and it is
what rules out a plotting library here.

The reading is: one lane per simulated machine, virtual time running left to
right, a dot for every scheduling decision that machine made, and an arrow
for every packet that crossed from one machine to another. A packet that was
sent and never arrived leaves a stub pointing nowhere, which is what a drop,
a loss and a partition all look like from the sender's side. Crashes,
restarts and packets held at transmission mark the lane they happened on.
"""

from __future__ import annotations

import html
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from simloop._trace import TraceEvent

# Events rendered by default. A 50,000-step campaign trace is a multi-megabyte
# page that stalls a browser, and the tail is the part a failure is in;
# ``limit=None`` renders everything for a caller who knows what they are
# asking for.
DEFAULT_LIMIT = 5000

# The verbs that name two ends of a link ("send a>b"). Anything else with a
# ">" in it is treated the same way, so a verb added later still draws.
_SEND = "send"
_DELIVER = "deliver"
# Verbs that end a packet's journey short: the sender's send becomes a stub.
_FATES = frozenset({"drop", "lost", "hold"})
# What happens to a machine rather than to a packet. These get the loud lane
# mark; everything else that marks a lane is a packet's small misfortune, and
# a run that drops a thousand datagrams must not look like a thousand crashes.
_MACHINE_FATES = frozenset({"crash", "restart"})
# "lost" marks the destination's lane. Two of the three places it is recorded
# earn that: a packet reaching a machine that is gone, and one reaching a port
# with nothing bound (both SimNetwork._deliver). The third does not — when a
# host crashes it sweeps the packets a standing cut was holding and records
# "lost" for every one where the crashed host is *either* end, so a crashed
# sender's held packets are charged to the lane they were addressed to rather
# than to the machine that died. The mark still lands on a lane the packet
# concerned, and the label carries "src>dst" either way. Every other fate —
# dropped at transmission, held by a standing cut, duplicated, released —
# happens at the sending end, so it marks the sender's lane.
_ARRIVAL_FATES = frozenset({"lost"})

# Layout, in user units of the SVG viewBox. The page is zoomable, so these
# are proportions rather than a promise about pixels.
_GUTTER = 160.0  # lane names live left of the plot
_PLOT = 1120.0
_RIGHT = 48.0
_HEAD = 56.0  # the time axis band
_FOOT = 40.0
_LANE = 64.0
# Dots are nudged off the lane's centre line by kind, so that a callback's
# scheduling and its running are two rows rather than one overprinted dot.
_KIND_OFFSET = {"schedule": -13.0, "run": 0.0, "cancel": 13.0, "advance": 0.0}
_DOT_RADIUS = 3.0
_MARK_REACH = 17.0  # half-height of a machine mark's tick
_FATE_REACH = 6.0  # half-height of a packet mark's much quieter tick
_STUB_REACH = 30.0  # how far a stub travels before it stops
_ARROW_INSET = 9.0  # gap between a lane's centre line and an arrow's end
_TICKS = 8


@dataclass(frozen=True, slots=True)
class _Packet:
    """One uid's journey: an arrow when it landed, a stub when it did not."""

    uid: int
    src: str
    dst: str
    start: float
    end: float
    fate: str

    @property
    def delivered(self) -> bool:
        return self.fate == _DELIVER


@dataclass(frozen=True, slots=True)
class _Mark:
    """Something that happened to one machine at one instant."""

    host: str
    verb: str
    when: float
    label: str


@dataclass(frozen=True, slots=True)
class _Scene:
    """Everything the renderer needs, with no TraceEvent left unbucketed."""

    hosts: tuple[str, ...]
    dots: dict[str, tuple[TraceEvent, ...]]
    marks: dict[str, tuple[_Mark, ...]]
    packets: tuple[_Packet, ...]
    start: float
    end: float
    shown: int


def timeline_html(
    events: Iterable[TraceEvent], *, limit: int | None = DEFAULT_LIMIT
) -> str:
    """Render ``events`` as a standalone HTML timeline.

    Returns the whole document, ready to write to a ``.html`` file. Only the
    last ``limit`` events are drawn — the page says so when it dropped any —
    and ``limit=None`` draws all of them.
    """
    ordered = list(events)
    total = len(ordered)
    if limit is not None:
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}")
        ordered = ordered[-limit:]
    return _render(_bucket(ordered), total)


# ----------------------------------------------------------------------
# Bucketing: pure functions from events to what gets drawn
# ----------------------------------------------------------------------


def _ends(label: str) -> tuple[str, str, str]:
    """A net label as ``(verb, src, dst)``; ``dst`` is "" for a lane event.

    ``"send a>b"`` names a link, ``"crash b"`` names a machine, and a label
    with neither shape names nothing at all.
    """
    verb, _, rest = label.partition(" ")
    src, link, dst = rest.partition(">")
    return (verb, src, dst if link else "")


def _lane_hosts(events: Sequence[TraceEvent]) -> tuple[str, ...]:
    """Which lanes to draw, in the order the events first mention them.

    Both ends of every packet get a lane even if that machine ran nothing in
    the window: an arrow needs somewhere to land. The simulation's own
    hostless work — clock advances, the network's delivery steps, and any
    network label that names no machine at all — gets one lane of its own,
    last, so that no event goes undrawn.
    """
    order: dict[str, None] = {}
    hostless = False
    for event in events:
        if event.kind == "net":
            _, src, dst = _ends(event.label)
            named = [name for name in (src, dst) if name]
            for name in named:
                order.setdefault(name, None)
            hostless = hostless or not named
        elif event.host:
            order.setdefault(event.host, None)
        else:
            hostless = True
    names = list(order)
    if hostless:
        names.append("")
    return tuple(names)


def _packets(events: Sequence[TraceEvent]) -> tuple[list[_Packet], list[_Mark]]:
    """Pair sends with deliveries per uid; everything unpaired still draws.

    One uid can be sent twice — a duplicated datagram is two sends and two
    deliveries, and a packet held by a partition is sent again after the heal
    — so sends queue per uid and the oldest one answers the next delivery.
    That pairs a re-sent packet's arrow with the send that actually crossed,
    and leaves the first send as a stub. A send that nothing ever answered is
    a packet still in flight when the run ended.
    """
    queued: dict[int, list[tuple[float, str, str]]] = {}
    packets: list[_Packet] = []
    marks: list[_Mark] = []
    for event in events:
        if event.kind != "net":
            continue
        verb, src, dst = _ends(event.label)
        if not dst:
            # Something that happened to one machine — a crash, a restart —
            # or, when the label names nobody, to the simulation at large.
            marks.append(_Mark(src, verb, event.when, event.label))
            continue
        queue = queued.setdefault(event.seq, [])
        if verb == _SEND:
            queue.append((event.when, src, dst))
        elif verb == _DELIVER:
            start = queue.pop(0)[0] if queue else event.when
            packets.append(
                _Packet(event.seq, src, dst, start, event.when, _DELIVER)
            )
        elif verb in _FATES and queue:
            packets.append(
                _Packet(event.seq, src, dst, queue.pop(0)[0], event.when, verb)
            )
        else:
            # A fate that struck before the packet was ever put on the wire
            # (held while the cut stood), a loss discovered at the far end
            # after the packet had already been drawn as delivered, or a verb
            # about a packet rather than about its journey. Where it happened
            # decides whose lane says so.
            where = dst if verb in _ARRIVAL_FATES else src
            marks.append(_Mark(where, verb, event.when, event.label))
    for uid, queue in queued.items():
        for when, src, dst in queue:
            packets.append(_Packet(uid, src, dst, when, when, "inflight"))
    packets.sort(key=lambda packet: (packet.start, packet.end, packet.uid))
    return packets, marks


def _bucket(events: Sequence[TraceEvent]) -> _Scene:
    hosts = _lane_hosts(events)
    packets, marks = _packets(events)
    dots: dict[str, list[TraceEvent]] = {host: [] for host in hosts}
    for event in events:
        if event.kind != "net":
            dots.setdefault(event.host, []).append(event)
    grouped: dict[str, list[_Mark]] = {host: [] for host in hosts}
    for mark in marks:
        grouped.setdefault(mark.host, []).append(mark)
    moments = [event.when for event in events]
    return _Scene(
        hosts=hosts,
        dots={host: tuple(found) for host, found in dots.items()},
        marks={host: tuple(found) for host, found in grouped.items()},
        packets=tuple(packets),
        start=min(moments, default=0.0),
        end=max(moments, default=0.0),
        shown=len(events),
    )


# ----------------------------------------------------------------------
# Rendering
# ----------------------------------------------------------------------


def _esc(text: str) -> str:
    return html.escape(text)


def _num(value: float) -> str:
    return f"{value:.2f}"


def _clock(when: float) -> str:
    return f"{when:.4f}"


def _name_of(host: str) -> str:
    # A display name for the lane holding what belongs to no machine. A host
    # could in principle be registered under this very name — host names
    # refuse only "|", ">" and newline — so the lane's identity is its empty
    # ``data-host``, which no machine can have, and never this label.
    return host if host else "the simulation"


def _title(*parts: str) -> str:
    body = "  ".join(part for part in parts if part)
    return f"<title>{_esc(body)}</title>"


def _render(scene: _Scene, total: int) -> str:
    width = _GUTTER + _PLOT + _RIGHT
    height = _HEAD + len(scene.hosts) * _LANE + _FOOT
    span = scene.end - scene.start

    def x_of(when: float) -> float:
        if span <= 0:
            # One instant, or none: the events all sit on the same line, and
            # a scale would be a division by zero either way.
            return _GUTTER + _PLOT / 2
        return _GUTTER + (when - scene.start) / span * _PLOT

    rows = {
        host: _HEAD + index * _LANE + _LANE / 2
        for index, host in enumerate(scene.hosts)
    }
    parts = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>simloop timeline</title>",
        f"<style>{_CSS}</style>",
        "</head>",
        "<body>",
        "<header>",
        "<h1>simloop timeline</h1>",
        f'<p class="meta">{_meta(scene)}</p>',
    ]
    if scene.shown < total:
        parts.append(
            f'<p class="banner">showing the last {scene.shown:,} of {total:,} '
            "events; everything earlier was dropped. Render with "
            "timeline_html(events, limit=None) for the whole run.</p>"
        )
    parts.append(_LEGEND)
    parts.append("</header>")
    parts.append(
        f'<svg id="sl-view" viewBox="0 0 {_num(width)} {_num(height)}" '
        f'data-events="{scene.shown}" data-total="{total}">'
    )
    parts.append(
        '<defs><marker id="sl-arrow" viewBox="0 0 8 8" refX="7" refY="4" '
        'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M 0 0 L 8 4 L 0 8 z"></path></marker></defs>'
    )
    parts.extend(_axis(scene, height, x_of))
    for index, host in enumerate(scene.hosts):
        parts.extend(_lane(scene, host, index, rows[host], width, x_of))
    parts.append('<g class="packets">')
    parts.extend(_packet(packet, rows, x_of) for packet in scene.packets)
    parts.append("</g>")
    parts.append("</svg>")
    if not scene.shown:
        parts.append('<p class="empty">no events: nothing was recorded</p>')
    parts.append(f"<script>{_JS}</script>")
    parts.append("</body>")
    parts.append("</html>")
    return "\n".join(parts) + "\n"


def _meta(scene: _Scene) -> str:
    lanes = len(scene.hosts)
    events = "event" if scene.shown == 1 else "events"
    noun = "lane" if lanes == 1 else "lanes"
    arrows = sum(1 for packet in scene.packets if packet.delivered)
    return _esc(
        f"{scene.shown:,} {events} across {lanes} {noun}, "
        f"{arrows:,} delivered, "
        f"virtual time {_clock(scene.start)} to {_clock(scene.end)}"
    )


def _axis(scene: _Scene, height: float, x_of: Callable[[float], float]) -> list[str]:
    span = scene.end - scene.start
    parts = ['<g class="axis">']
    for step in range(_TICKS + 1):
        when = scene.start + span * step / _TICKS
        x = x_of(when)
        parts.append(
            f'<line class="grid" x1="{_num(x)}" y1="{_num(_HEAD - 18)}" '
            f'x2="{_num(x)}" y2="{_num(height - _FOOT + 8)}"></line>'
        )
        parts.append(
            f'<text class="tick" x="{_num(x)}" y="{_num(_HEAD - 26)}">'
            f"t={_clock(when)}</text>"
        )
    parts.append("</g>")
    return parts


def _lane(
    scene: _Scene,
    host: str,
    index: int,
    row: float,
    width: float,
    x_of: Callable[[float], float],
) -> list[str]:
    shade = " lane-odd" if index % 2 else ""
    sim = " lane-sim" if not host else ""
    parts = [
        f'<g class="lane" data-host="{_esc(host)}" data-row="{index}">',
        f'<rect class="lane-bg{shade}{sim}" x="0" y="{_num(row - _LANE / 2)}" '
        f'width="{_num(width)}" height="{_num(_LANE)}"></rect>',
        f'<line class="lane-line" x1="{_num(_GUTTER)}" y1="{_num(row)}" '
        f'x2="{_num(width - _RIGHT / 2)}" y2="{_num(row)}"></line>',
        f'<text class="lane-name" x="14" y="{_num(row + 4)}">'
        f"{_esc(_name_of(host))}</text>",
    ]
    for event in scene.dots.get(host, ()):
        y = row + _KIND_OFFSET.get(event.kind, 0.0)
        x = x_of(event.when)
        when = f"t={_clock(event.when)}"
        parts.append(
            f'<circle class="dot" data-kind="{_esc(event.kind)}" '
            f'cx="{_num(x)}" cy="{_num(y)}" r="{_DOT_RADIUS}">'
            f"{_title(when, event.kind, host, event.label)}"
            "</circle>"
        )
    for mark in scene.marks.get(host, ()):
        x = x_of(mark.when)
        machine = mark.verb in _MACHINE_FATES
        # A machine's fate and a packet's fate are different news, so they are
        # different marks: a full-height tick for a crash or a restart, and a
        # small quiet one for a packet that was dropped, held or duplicated.
        css, reach = ("mark", _MARK_REACH) if machine else ("fate", _FATE_REACH)
        parts.append(
            f'<line class="{css}" data-mark="{_esc(mark.verb)}" '
            f'data-host="{_esc(host)}" data-when="{mark.when!r}" '
            f'x1="{_num(x)}" y1="{_num(row - reach)}" '
            f'x2="{_num(x)}" y2="{_num(row + reach)}">'
            f"{_title(f't={_clock(mark.when)}', mark.label)}</line>"
        )
    parts.append("</g>")
    return parts


def _packet(
    packet: _Packet, rows: dict[str, float], x_of: Callable[[float], float]
) -> str:
    src = rows.get(packet.src, 0.0)
    dst = rows.get(packet.dst, src)
    # Down the page, up the page, or — a machine talking to itself — neither.
    heading = (dst > src) - (dst < src)
    left = x_of(packet.start)
    link = f"{packet.src}>{packet.dst}"
    common = (
        f'data-uid="{packet.uid}" data-src="{_esc(packet.src)}" '
        f'data-dst="{_esc(packet.dst)}" data-start="{packet.start!r}" '
        f'data-end="{packet.end!r}"'
    )
    when = f"t={_clock(packet.start)}"
    if packet.delivered:
        # Both ends stop short of the lane lines, so that an arrowhead does
        # not land on top of the dots it arrived among. A self-addressed
        # packet has no lane to travel to, and hops above its own instead.
        inset = _ARROW_INSET * heading if heading else -_ARROW_INSET
        return (
            f'<line class="arrow" {common} x1="{_num(left)}" '
            f'y1="{_num(src + inset)}" x2="{_num(x_of(packet.end))}" '
            f'y2="{_num(dst - inset if heading else src + inset)}" '
            'marker-end="url(#sl-arrow)">'
            f"{_title(when, link, f'delivered t={_clock(packet.end)}')}</line>"
        )
    fate = (
        "still in flight when the run ended"
        if packet.fate == "inflight"
        else f"{packet.fate} t={_clock(packet.end)}"
    )
    drift = _STUB_REACH / 2 * (heading if heading else -1)
    return (
        f'<line class="stub" data-fate="{_esc(packet.fate)}" {common} '
        f'x1="{_num(left)}" y1="{_num(src)}" '
        f'x2="{_num(left + _STUB_REACH)}" y2="{_num(src + drift)}">'
        f"{_title(when, link, fate)}</line>"
    )


_LEGEND = (
    '<ul class="legend">'
    '<li><span class="key key-schedule"></span>scheduled</li>'
    '<li><span class="key key-run"></span>ran</li>'
    '<li><span class="key key-cancel"></span>cancelled</li>'
    '<li><span class="key key-arrow"></span>packet delivered</li>'
    '<li><span class="key key-stub"></span>sent, never arrived</li>'
    '<li><span class="key key-fate"></span>packet dropped, held, '
    "duplicated or lost</li>"
    '<li><span class="key key-mark"></span>machine crashed or restarted</li>'
    "</ul>"
    '<p class="hint">wheel to zoom, drag to pan, double-click to reset; '
    "hover any element for its label and time</p>"
)

_CSS = """
:root {
  --bg: #fbfbfd; --panel: #fff; --ink: #1c1f26; --muted: #6b7280;
  --rule: #e2e5ec; --band: #f3f5f9; --sim: #eef1f6;
  --run: #2563eb; --schedule: #93a3bd; --cancel: #b91c1c;
  --arrow: #0f766e; --stub: #d97706; --mark: #7c3aed;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14161c; --panel: #191c24; --ink: #e8eaf0; --muted: #97a0b3;
    --rule: #2b303c; --band: #1e222b; --sim: #232833;
    --run: #7aa2f7; --schedule: #5c6b8a; --cancel: #f7768e;
    --arrow: #4bc4b4; --stub: #e0a35c; --mark: #bb9af7;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 20px 24px 40px; background: var(--bg); color: var(--ink);
  font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
}
h1 { font-size: 17px; margin: 0 0 4px; letter-spacing: 0.01em; }
.meta, .hint, .empty { color: var(--muted); margin: 0 0 6px; font-size: 13px; }
.banner {
  margin: 8px 0; padding: 8px 12px; border-radius: 6px;
  border: 1px solid var(--stub); color: var(--ink); background: var(--band);
  font-size: 13px;
}
.legend { display: flex; flex-wrap: wrap; gap: 14px; list-style: none;
  margin: 10px 0 6px; padding: 0; font-size: 12px; color: var(--muted); }
.legend li { display: flex; align-items: center; gap: 6px; }
.key { display: inline-block; width: 12px; height: 12px; border-radius: 50%; }
.key-schedule { background: var(--schedule); }
.key-run { background: var(--run); }
.key-cancel { background: var(--cancel); }
.key-arrow { height: 0; border-top: 2px solid var(--arrow); border-radius: 0; }
.key-stub { height: 0; border-top: 2px dashed var(--stub); border-radius: 0; }
.key-mark { width: 3px; height: 14px; border-radius: 1px; background: var(--mark); }
.key-fate { width: 3px; height: 7px; border-radius: 1px; background: var(--stub); }
svg {
  display: block; width: 100%; height: auto; margin-top: 6px;
  background: var(--panel); border: 1px solid var(--rule); border-radius: 8px;
  touch-action: none; cursor: grab;
}
svg.dragging { cursor: grabbing; }
.lane-bg { fill: var(--panel); }
.lane-odd { fill: var(--band); }
.lane-sim { fill: var(--sim); }
.lane-line { stroke: var(--rule); stroke-width: 1; }
.lane-name {
  fill: var(--ink); font-size: 13px; font-weight: 600;
  font-family: ui-monospace, "SF Mono", Menlo, monospace;
}
.grid { stroke: var(--rule); stroke-width: 1; stroke-dasharray: 2 4; }
.tick {
  fill: var(--muted); font-size: 11px; text-anchor: middle;
  font-family: ui-monospace, "SF Mono", Menlo, monospace;
}
.dot { fill: var(--run); }
.dot[data-kind="schedule"] { fill: var(--schedule); }
.dot[data-kind="cancel"] { fill: var(--cancel); }
.dot[data-kind="advance"] { fill: none; stroke: var(--muted); stroke-width: 1.5; }
.arrow { stroke: var(--arrow); stroke-width: 1.6; }
.stub { stroke: var(--stub); stroke-width: 1.6; stroke-dasharray: 3 3; }
.mark { stroke: var(--mark); stroke-width: 2.5; }
.mark[data-mark="restart"] { stroke-dasharray: 3 3; }
.fate { stroke: var(--stub); stroke-width: 1.4; }
.fate[data-mark="dup"], .fate[data-mark="release"] { stroke: var(--muted); }
marker path { fill: var(--arrow); }
"""

_JS = """
(function () {
  var svg = document.getElementById("sl-view");
  if (!svg) { return; }
  var home = svg.getAttribute("viewBox").split(" ").map(Number);
  var view = home.slice();
  var grabbed = null;
  function apply() { svg.setAttribute("viewBox", view.join(" ")); }
  function at(event) {
    var box = svg.getBoundingClientRect();
    return [
      view[0] + (event.clientX - box.left) / box.width * view[2],
      view[1] + (event.clientY - box.top) / box.height * view[3]
    ];
  }
  svg.addEventListener("wheel", function (event) {
    event.preventDefault();
    var factor = event.deltaY < 0 ? 0.85 : 1.18;
    var width = view[2] * factor;
    if (width > home[2] * 3 || width < home[2] / 400) { return; }
    var spot = at(event);
    view[0] = spot[0] - (spot[0] - view[0]) * factor;
    view[1] = spot[1] - (spot[1] - view[1]) * factor;
    view[2] = width;
    view[3] = view[3] * factor;
    apply();
  }, { passive: false });
  svg.addEventListener("pointerdown", function (event) {
    grabbed = at(event);
    svg.setPointerCapture(event.pointerId);
    svg.classList.add("dragging");
  });
  svg.addEventListener("pointermove", function (event) {
    if (!grabbed) { return; }
    var spot = at(event);
    view[0] -= spot[0] - grabbed[0];
    view[1] -= spot[1] - grabbed[1];
    apply();
  });
  function release() { grabbed = null; svg.classList.remove("dragging"); }
  svg.addEventListener("pointerup", release);
  svg.addEventListener("pointercancel", release);
  svg.addEventListener("dblclick", function () {
    view = home.slice();
    apply();
  });
})();
"""
