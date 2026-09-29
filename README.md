# simloop

[![CI](https://github.com/dhruvl/simloop/actions/workflows/ci.yml/badge.svg)](https://github.com/dhruvl/simloop/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/simloop)](https://pypi.org/project/simloop/)

simloop is a Python event loop similar to the standard asyncio loop, but
built specifically for deterministic simulation testing using techniques from
FoundationDB and TigerBeetle.

The simulator systematically injects latency, drops packets, and crashes hosts
to surface bugs. When a test fails, you get a seed. Using that seed, the exact
same race condition reproduces every single time. The
[limits](https://github.com/dhruvl/simloop#limits) are listed below.

Documentation: [dhruvl.github.io/simloop](https://dhruvl.github.io/simloop/).

## Install

```
pip install simloop
```

Python 3.12+. No runtime dependencies. The pytest plugin ships in the
same package and activates automatically.

A sim test is an `async def` test under `@sim_test`. It runs on a simulated
loop, so five minutes of sleeping costs nothing:

```python
import asyncio

from simloop import sim_test


@sim_test
async def test_virtual_time_is_free():
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.sleep(300)
    assert loop.time() - start == 300
```

It sits alongside the rest of a suite, pytest-asyncio tests included; the
[quickstart](https://github.com/dhruvl/simloop/blob/main/docs/quickstart.md) goes from here to a replayed failure
one command at a time.

## Find a bug, then replay it

A counter service, and two clients that each read the count and write back
one more. It is the textbook lost update, over the wire:

```python
import asyncio
from simloop import sim_test


@sim_test(seeds=200)
async def test_two_increments_both_count():
    loop = asyncio.get_running_loop()
    loop.net.set_defaults(latency=(0.001, 0.050))
    count = 0

    async def serve():
        async def handle(reader, writer):
            nonlocal count
            line = await reader.readline()
            if line == b"get\n":
                writer.write(b"%d\n" % count)
            else:
                count = int(line)
                writer.write(b"ok\n")
            writer.close()

        server = await asyncio.start_server(handle, port=8080)
        async with server:
            await server.serve_forever()

    async def call(line):
        reader, writer = await asyncio.open_connection("counter", 8080)
        writer.write(line)
        reply = await reader.readline()
        writer.close()
        return reply

    async def increment():
        seen = int(await call(b"get\n"))
        await call(b"%d\n" % (seen + 1))

    loop.net.host("counter").create_task(serve())
    await asyncio.sleep(1.0)

    first = loop.net.host("a").create_task(increment())
    await asyncio.sleep(0.2)
    second = loop.net.host("b").create_task(increment())
    await asyncio.gather(first, second)
    assert count == 2
```

The second client starts 200 ms after the first, so on most latency draws
the increments serialize and the test passes. `@sim_test(seeds=200)` runs
the test under 200 seeds, each on a fresh simulated loop, and stops at the
one where they overlap:

```
simloop: failed at seed 128 (128 seeds passed first)
replay: pytest 'tests/test_counter.py::test_two_increments_both_count' --simloop-replay=128

last 20 trace events:
  [t=1.3501] net      seq=22  deliver counter>b
  [t=1.3944] advance  seq=-1
  [t=1.3944] run      seq=58  SimNetwork._deliver
  [t=1.3944] net      seq=21  deliver counter>b
  [t=1.3944] schedule seq=63  b  Task.task_wakeup
  [t=1.3944] run      seq=63  b  Task.task_wakeup
  [t=1.3944] net      seq=23  send b>counter
  ...
  [t=1.3944] run      seq=70  driver  SimLoop._stop_when_done

runs agree for 41 events; passing then ran StreamReaderProtocol.connection_made.<locals>.callback, failing ran list.remove
passing run:
  [t=1.0944] schedule seq=13  counter  SimNetwork._deliver
  ...
failing run:
  [t=1.1224] schedule seq=13  counter  SimNetwork._deliver
  ...
pending tasks by host:
  counter  Task 'Task-1026'  awaiting serve  at tests/test_counter.py:24
```

The report names the failing seed and the command that replays it, then
diffs the failing run against the last passing seed, so the first thing the
two runs did differently is one line of output. Each trace line names the
machine whose work it was. Lines that name none belong to the simulation
itself, such as the clock advancing or a packet crossing the wire. In CI,
raise the seed count without touching code:

```
pytest --simloop-seeds=1000
```

Uniform draws are not the only search: `--simloop-policy=pct` switches to
PCT scheduling (Burckhardt et al., ASPLOS 2010), which guarantees a lower bound
on the chance of finding deeper ordering bugs. It is a floor, not a speed-up;
the bound and the measurement are on the
[contract page](https://github.com/dhruvl/simloop/blob/main/docs/supported-api.md#scheduling-policies).

## Shrink the schedule to the race

Most steps of a failing schedule are noise. With `--simloop-shrink`, the
explorer replays edited copies of the recorded schedule, walking it back
toward plain FIFO order until only the decisions that reproduce the
failure remain. What is left names the racing steps:

```python
@sim_test(seeds=50)
async def test_the_audit_sees_every_deposit():
    loop = asyncio.get_running_loop()
    ledger = {"balance": 0}
    audited = []

    def deposit():
        ledger["balance"] += 1

    def audit():
        audited.append(ledger["balance"])

    async def chatter():
        for _ in range(20):
            await asyncio.sleep(0.001)

    background = [loop.create_task(chatter()) for _ in range(3)]
    await asyncio.sleep(0.005)
    loop.call_soon(deposit)
    loop.call_soon(audit)
    await asyncio.gather(*background)
    assert audited == [1]
```

```
schedule shrink (experimental): 137 steps recorded, 14 runs to minimize
minimized: FIFO except step 36
  step 36  test_the_audit_sees_every_deposit.<locals>.audit
```

One step out of 137 had to go a specific way: the audit ran before the
deposit it should have seen. If shrinking reports `minimized: FIFO
throughout` instead, the interleaving never mattered and the fault timings
are the place to look. Shrinking is experimental and off by default, since
it costs extra runs, capped by `--simloop-shrink-budget` (default 500).

## Watch the run that failed

A schedule is easier to read as a picture than as a wall of trace lines:

```
pytest --simloop-timeline=artifacts
```

Every failing seed leaves `artifacts/simloop-timeline-seed<N>.html`, named in
its failure report. The page is self-contained, with its CSS, script and
SVG inline and nothing fetched, so it opens from a CI artifact store or an
attachment as readily as from disk. It draws one lane per simulated machine
and one for the simulation itself, virtual time running left to right, a dot
for every scheduling decision on that machine, and an arrow for every packet
that crossed. A packet that was sent and never arrived leaves a stub pointing
nowhere, which is what a drop, a loss and a partition all look like from the
sender's side; crashes and restarts mark the lane they struck. The last 5,000
events are drawn, and the page says so when there were more.
`simloop.timeline_html(events)` renders the same page from any trace you are
holding.

## Using with Hypothesis

Hypothesis picks inputs and simloop picks interleavings, so the two stack.
`@given` builds the workload, `explore()` runs it under a range of seeds, and
the property is "no seed broke it":

```python
@settings(deadline=None, derandomize=True, database=None)
@given(writers=st.integers(1, 4), payloads=st.lists(st.text(), min_size=1))
def test_the_log_keeps_every_append(writers, payloads):
    report = explore(lambda: replicate(writers, payloads), range(8))
    assert report is None, report.render()
```

A failure then arrives in two halves: Hypothesis reports the smallest workload
that still breaks, simloop reports the seed that breaks it and the command
that replays it. Seeds stay out of the strategies on purpose, because a seed
has no size to shrink toward and two shrinkers aimed at one failure fight.
The worked example, the settings CI needs and the limits are in
[docs/cookbook.md](https://github.com/dhruvl/simloop/blob/main/docs/cookbook.md), and the same composition runs as a
test in this repository. simloop has no Hypothesis dependency and no
integration package.

## How it compares

pytest-asyncio is what most asyncio suites already run on, and simloop does
not replace it. It runs tests on the real event loop, so real sockets,
threads and subprocesses all work, which makes it the right tool for most of
a suite. The two coexist in one run: a `@sim_test` is an ordinary
synchronous test as far as pytest is concerned, so pytest-asyncio leaves it
alone in both strict and auto mode. Reach for simloop on the tests where the
interleaving or the network is the thing being tested.

trio's autojump clock gives trio code virtual time and ships with trio itself.
How trio approached deterministic scheduling, and what simloop took from it,
is in [docs/design.md](https://github.com/dhruvl/simloop/blob/main/docs/design.md#the-gap).

Antithesis runs a whole deployment, in any language, under a deterministic
hypervisor, so it sees code that never touches an event loop. simloop sees
one Python process, and only what passes through asyncio.

## Features

Scheduling is seeded. The ready queue runs in an order drawn from a per-run
PRNG, so a seed pins the whole interleaving. Time is virtual, so
`asyncio.sleep(300)` costs nothing and timeouts fire in simulated seconds.

The network is simulated as well. `open_connection`, `start_server` and
datagram endpoints run over an in-memory packet core with per-link latency,
drop and duplication, plus partitions and host crashes:

```python
net = loop.net
net.set_defaults(latency=(0.001, 0.010), drop=0.05)
net.partition({"node1"}, {"node2", "node3"})   # silent blackhole
loop.call_later(5.0, net.heal)                  # heals in virtual time
net.crash("node2")                              # no reset, just silence
```

A crashed host can come back. `net.crash` kills a host's tasks and binds,
`net.restart` brings it back as a fresh incarnation, and `host.disk` is a
mapping that outlives both, so machines die, reboot and remember what they
wrote down. `net.set_disk(name, buffered=True, torn=True)` makes the disk
misbehave too: writes only land on `sync()`, and a crash keeps a seeded
prefix of whatever was still queued. `net.set_clock(name, offset=...)` skews
what one host reads from the clock without changing how long anything takes,
for testing lease and timeout code against machines that disagree about the
time.

Every scheduling and fault decision lands in an append-only trace, and two
runs with the same trace hash made the same decisions in the same order.
Code under test can still use randomness and clocks: `sim.random`,
`sim.uuid4()` and `sim.time()` draw from seed-derived streams inside a run
and fall back to the standard library outside one.

## Two demos

`examples/jobqueue/` is a complete distributed system in plain asyncio: an
exactly-once job scheduler with leases, fencing tokens, idempotency keys,
backoff and dead-lettering, tested end to end with simloop. Its suite runs
hundreds of seeds of partitions, crashes and poison jobs, plus six
hand-picked mutations that each switch off one safeguard. The explorer finds
a violation for every one of them and replays it from a seed. The bug table
is in [examples/jobqueue/README.md](https://github.com/dhruvl/simloop/blob/main/examples/jobqueue/README.md).

`examples/raft/` is a teaching-sized Raft, leader election and log
replication in plain asyncio on streams, swept under 50,000 seeds of
partitions, crashed-and-restarted processes and message loss. Four safety
invariants hold under every schedule the explorer reaches. Switch off one of
the vote ledger, the log-freshness check, the commit gate,
persistence-before-reply or stale-term rejection and the explorer finds a
seed-replayable violation, then minimizes the failing schedule to the steps
that had to go a particular way. In the sharpest case that is one step out
of 3,514. The table is in
[examples/raft/README.md](https://github.com/dhruvl/simloop/blob/main/examples/raft/README.md).

## Performance

Replayable scheduling costs nothing at test time. On a ring of 100 tasks
passing a token, SimLoop spends 4.49 µs of CPU per scheduling step, trace
recording included, against 5.18 µs for the stock loop and 3.44 µs for
uvloop. By wall clock it finishes in under a third of the time either real
loop takes, because on macOS both of them wait in kqueue on every iteration
and SimLoop never makes that call. That is a saving at test time, not a
faster event loop: uvloop still does less work per step.

Virtual time compresses sleep-heavy workloads about 2,000× against wall
clock (1,775–2,040× across nine runs). The jobqueue chaos scenario runs at
about 55 seeds/s in one process (300 seeds in 5.2–6.3 s across nine runs).
Across ten processes a 100,000-seed sweep takes 3.5–3.8 minutes, 443–473
seeds/s over three runs: 8.7 times one process, on a laptop with four
performance cores and six efficiency cores. All of it was measured on an M4
MacBook Air in one sitting, and wall-clock throughput there moves by double
digits between sittings; the methodology and the reasons are in
[benchmarks/README.md](https://github.com/dhruvl/simloop/blob/main/benchmarks/README.md).

## Limits

Code that goes through the event-loop API is supported. Code that bypasses
it is fenced: threads, raw socket reads and writes, subprocesses and signals
raise `SimulationFenceError` rather than silently breaking determinism.

A few things that look as if they would fence do not. `run_in_executor` runs
the function inline at a seeded scheduling step, with no pool and no thread,
so `asyncio.to_thread` works. `call_soon_threadsafe` is `call_soon` when the
caller is the loop's own thread, while a real second thread still fences.
`sock_connect` on an `AF_INET` stream socket is simulated, so a client that
connects a socket and hands it to `create_connection` runs; the datagram and
raw variants still fence. `getaddrinfo` resolves sim host names to stable
synthetic addresses and raises `socket.gaierror` for anything else, and no
real DNS lookup is ever made.

TLS runs a real handshake through the standard library's `SSLProtocol` over
a pair of memory BIOs, with real certificate verification and no descriptor
or wall clock. Each flight is one simulated packet paying the link's latency,
and a handshake deadline fires in virtual time. Write-side flow control is
simulated on request: `net.set_flow_control()` makes `drain()` wait while the
peer has not read, so backpressure deadlocks and missing pause/resume
handling become findable. It is off by default, and a run that does not ask
for it makes the same decisions it always did.

The full contract is in [docs/supported-api.md](https://github.com/dhruvl/simloop/blob/main/docs/supported-api.md).
[docs/compatibility.md](https://github.com/dhruvl/simloop/blob/main/docs/compatibility.md) records what aiohttp,
anyio, websockets and httpx actually do when run under simulation.

## Design

The simulated loop is one file, about 600 lines of code implementing
`AbstractEventLoop` from scratch rather than instrumenting the stock loop.
Why it was built that way, why one seed feeds three separate RNG streams,
why streams never lose bytes but datagrams do: the decisions, and the
alternatives they beat, are written up in
[docs/design.md](https://github.com/dhruvl/simloop/blob/main/docs/design.md).

## License

MIT. Maintained by Dhruv Kumar Singh; see
[CONTRIBUTING.md](https://github.com/dhruvl/simloop/blob/main/CONTRIBUTING.md) to send a change and
[SECURITY.md](https://github.com/dhruvl/simloop/blob/main/SECURITY.md) to report a vulnerability.
