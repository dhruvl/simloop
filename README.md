# simloop

Deterministic simulation testing for Python asyncio: seeded scheduling,
virtual time, and a simulated network with fault injection. Any failure
simloop finds replays exactly from a seed.

Rust has [madsim](https://github.com/madsim-rs/madsim) and
[turmoil](https://github.com/tokio-rs/turmoil); FoundationDB and
TigerBeetle made the technique famous. simloop brings it to asyncio:
your real, unmodified networking code runs on a simulated loop where
time is virtual, every scheduling decision is seeded, and the network
loses, delays, duplicates, and partitions traffic on command.

## Install

```
pip install simloop
```

Python 3.12+. No runtime dependencies. The pytest plugin ships in the
same package and activates automatically.

## Find a bug, then replay it

A counter service and two clients that each read the count and write back
one more — the textbook lost update, over the wire:

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
  [t=1.3944] run      seq=58  SimNetwork._deliver
  [t=1.3944] net      seq=23  send b>counter
  ...
  [t=1.3944] run      seq=70  SimLoop._stop_when_done

runs agree for 38 events; passing then ran StreamReaderProtocol.connection_made.<locals>.callback, failing ran list.remove
passing run:
  [t=1.0944] schedule seq=13  SimNetwork._deliver
  ...
failing run:
  [t=1.1224] schedule seq=13  SimNetwork._deliver
  ...
pending tasks by host:
  counter  Task 'Task-1026'  awaiting serve  at tests/test_counter.py:26
```

The report names the failing seed, prints the exact command that replays
it — same scheduling decisions, same fault decisions, same trace — and
diffs the failing run against the last passing seed, so the first thing
the two runs did differently is one line of output. In CI, crank the
search without touching code:

```
pytest --simloop-seeds=1000
```

## Shrink the schedule to the race

Most steps of a failing schedule are noise. With `--simloop-shrink`, the
explorer replays edited copies of the recorded schedule, walking it back
toward plain FIFO order until only the decisions that reproduce the
failure remain — so the racing steps come out named:

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
deposit it should have seen. When shrinking instead reports `minimized:
FIFO throughout`, that is an answer too — the interleaving never mattered,
so look at the fault timings, not the task order. Shrinking is off by
default (it costs extra runs, capped by `--simloop-shrink-budget`, default
500) and experimental.

## What the simulation gives you

- **Seeded scheduling** — the ready queue's execution order comes from a
  per-run PRNG; a seed pins the entire interleaving.
- **Virtual time** — `asyncio.sleep(300)` costs nothing; timeouts fire
  in simulated seconds.
- **A simulated network** — `open_connection` / `start_server` /
  datagram endpoints run over an in-memory packet core with per-link
  latency, drop, and duplication, plus partitions and host crashes:

```python
net = loop.net
net.set_defaults(latency=(0.001, 0.010), drop=0.05)
net.partition({"node1"}, {"node2", "node3"})   # silent blackhole
loop.call_later(5.0, net.heal)                  # heals in virtual time
net.crash("node2")                              # no reset, just silence
```

- **Crashes with a way back** — `net.crash` kills a host's tasks and
  binds, `net.restart` brings it back as a fresh incarnation, and
  `host.disk` is a mapping that outlives both: machines die, reboot, and
  remember what they wrote down.
- **Clocks that lie** — `net.set_clock(name, offset=...)` skews what one
  host reads from the clock without changing how long anything takes, for
  testing lease and timeout code against machines that disagree about the
  time.
- **Replayable traces** — every scheduling and fault decision lands in
  an append-only trace whose hash proves a replay is exact.
- **Seeded stand-ins** — `sim.random`, `sim.uuid4()` and `sim.time()`
  draw from seed-derived streams inside a run and fall back to the
  stdlib outside one, so code under test can use entropy and clocks
  without breaking replay.

## Proving it: the demos

`examples/jobqueue/` is a complete distributed system — an exactly-once
job scheduler (leases, fencing tokens, idempotency keys, backoff, and
dead-lettering) written in plain asyncio — tested end to end with
simloop. Its suite runs hundreds of seeds of partitions, crashes, and
poison jobs, and shows that removing any load-bearing safeguard produces
a violation the explorer finds and replays from a seed. The bug table
lives in [examples/jobqueue/README.md](https://github.com/dhruvl/simloop/blob/main/examples/jobqueue/README.md).

`examples/raft/` is the second proof: a teaching-sized Raft — leader
election and log replication in plain asyncio on streams — swept under
50,000 seeds of partitions, crashed-and-restarted processes, and message
loss. Four safety invariants hold on every seed; remove any safeguard
(the vote ledger, the log-freshness check, the commit gate,
persistence-before-reply, stale-term rejection) and the explorer finds a
seed-replayable violation, then minimizes the failing schedule to the
steps that had to go a particular way — one step out of 3,514 recorded, in
the sharpest case. The table lives in
[examples/raft/README.md](https://github.com/dhruvl/simloop/blob/main/examples/raft/README.md).

## Performance

Simulation is cheap: SimLoop schedules a task step in ~4.4 µs (trace
recording included — about 3.6× faster than the stock loop, which pays a
selector syscall per iteration), compresses sleep-heavy workloads ~2,000×
against wall clock, and with `--simloop-jobs` fanning seeds across
processes, the full jobqueue chaos scenario sweeps 100,000 seeds in six
minutes (~280 seeds/second) on an M4 MacBook Air. Methodology, numbers,
and the campaign results:
[benchmarks/README.md](https://github.com/dhruvl/simloop/blob/main/benchmarks/README.md).

## Honest limits

Code that goes through the event-loop API is supported; code that
bypasses it is fenced: threads and executors, raw socket reads and
writes, subprocesses, signals, and loop-level TLS upgrades raise
`SimulationFenceError` rather than silently breaking determinism.
`sock_connect` on an `AF_INET` stream socket is the exception — it is
simulated, so a client that connects a socket and hands it to
`create_connection` runs, while the datagram and raw variants still fence.
Name resolution stays inside the simulation: `getaddrinfo` resolves sim
host names to stable synthetic addresses and raises `socket.gaierror` for
anything else — no real DNS, ever.
Write-side flow control is not simulated.
The full contract is in [docs/supported-api.md](https://github.com/dhruvl/simloop/blob/main/docs/supported-api.md).
What that contract costs real libraries — what aiohttp, anyio, websockets and
httpx actually do under simulation, measured rather than promised — is in
[docs/compatibility.md](https://github.com/dhruvl/simloop/blob/main/docs/compatibility.md).

## Design

Why the loop is a from-scratch `AbstractEventLoop` instead of an
instrumented stock loop, why one seed feeds three separate RNG streams,
why streams never lose bytes but datagrams do — the decisions and the
alternatives they beat are written up in
[docs/design.md](https://github.com/dhruvl/simloop/blob/main/docs/design.md).

## License

MIT
