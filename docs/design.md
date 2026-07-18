# simloop design notes

How simloop works, and — more useful — why it works this way. Every section
that describes a decision also names the alternatives it beat and what would
have gone wrong with them. For the user-facing contract see
[supported-api.md](supported-api.md); this page is about the machinery.

## The gap

Deterministic simulation testing has a distinguished lineage — FoundationDB
built its reputation on it, TigerBeetle and Antithesis made it a selling
point, and Rust has two mature frameworks in
[madsim](https://github.com/madsim-rs/madsim) and
[turmoil](https://github.com/tokio-rs/turmoil). Python had the pieces but
never the whole: [looptime](https://github.com/nolar/looptime) and trio's
autojump clock do virtual time only, an abandoned Ethereum project mocked
asyncio sockets without determinism, and anysystem runs simulated processes
against its own API rather than real asyncio code.

The closest anyone came is instructive. Trio users asked for tools to find
scheduler-dependent heisenbugs in 2017
([trio#239](https://github.com/python-trio/trio/issues/239)); deterministic
scheduling landed in 2019 as an undocumented internal hook
([trio#890](https://github.com/python-trio/trio/pull/890)) after the
maintainers measured ~15% overhead for instrumenting the live scheduler; in
2021 users were still asking for a public version
([trio#2022](https://github.com/python-trio/trio/issues/2022)). Two lessons
carried into simloop: instrumentation must cost nothing when it is not in
use, and a deterministic mode bolted onto a production scheduler fights that
scheduler forever.

simloop's answer is to not touch the production loop at all. The simulation
is a separate `asyncio.AbstractEventLoop` implementation you only construct
in tests; production code runs on stock asyncio, unchanged and uninstrumented.

## The core bet: `call_soon` is the only door

Everything asyncio schedules goes through the loop's `call_soon` / `call_at`
family. Crucially that includes coroutines: `asyncio.Task` drives every step
of every coroutine by scheduling its `__step` callback via `call_soon`. Own
dispatch and you own task interleaving — without touching `Task`, without
instrumenting await points, without import hooks.

So `SimLoop` implements `AbstractEventLoop` directly (~500 lines,
`src/simloop/_loop.py`) around three structures: a list of ready callbacks, a
timer heap, and a virtual clock starting at 0.0. The scheduling core is one
method:

- If callbacks are ready, draw the next one to run from a seeded
  `random.Random` — this draw is **the only source of nondeterminism in the
  entire loop**, so a seed pins the entire execution.
- If none are ready, jump the clock to the earliest timer deadline and move
  every timer due at that instant onto the ready list. Time never advances
  while work is pending, and never waits on the wall clock.
- If there are no ready callbacks *and* no timers, nothing can ever run
  again: raise `SimulationDeadlockError`. Lost-wakeup bugs surface as a
  diagnosis instead of a hang.

Alternatives considered and rejected:

- **Subclassing `BaseEventLoop`** — it is welded to a selector; every
  iteration wants to poll the OS. Fighting that buys nothing, since the
  simulation has no file descriptors.
- **Instrumenting the stock loop** (the trio#890 shape) — pays overhead in
  production or demands an opt-in mode inside code you don't control, and
  the loop's real selector timing keeps leaking into test behavior.
- **Randomizing elsewhere** (task creation order, fuzzing timer durations) —
  either misses interleavings or distorts the program's actual timing.
  Permuting the ready queue explores exactly the reorderings asyncio itself
  is allowed to perform: anything simloop finds is a legal schedule of your
  program.

## Deterministic identity, or: never compare objects

A seeded draw only helps if everything around it is order-stable too. Three
rules hold everywhere:

1. **Every scheduled callback gets a `seq`** from a global counter at
   scheduling time. The timer heap is keyed `(deadline, seq)` so equal
   deadlines break ties by creation order — handles themselves are never
   compared, and no scheduling decision ever depends on `id()`.
2. **Trace labels are qualified names, never `repr()`** — a repr can embed a
   memory address, which would make traces differ between identical runs.
3. **No scheduling decision iterates a set or hash-ordered mapping.** The
   loop's structures are lists, heaps, and insertion-ordered dicts.

This is what makes the replay proof cross-process. The hardening suite
(`tests/test_hardening.py`, `tests/test_net_hardening.py`) re-runs reference
workloads 100 times per seed in-process and then again in fresh interpreters
under *different* `PYTHONHASHSEED` values, asserting bit-identical trace
hashes throughout.

## The trace is the proof

Every decision lands in an append-only recorder (`src/simloop/_trace.py`):
schedule, run, clock advance, cancellation, and every network verdict. Two
properties are load-bearing:

- **Completeness.** Even a *cancelled* handle's draw is recorded — the draw
  consumed PRNG state, so it is a scheduling decision, and a replay that made
  a different set of draws must produce a different hash.
- **Injectivity.** Events serialize as `kind|when|seq|label` lines into a
  SHA-256. Labels are qualified callback names or network labels built from
  validated host names, and host names may not contain `|`, `>` or newline —
  so two distinct event streams cannot collide onto one byte sequence.

Hash equality is therefore a cheap, sufficient check that a replay was
*exact*, not merely same-outcome. It is asserted all over the test suite and
printed with every failure report.

## Three RNG streams, never one

`SimLoop` derives three independent generators from one seed: the
scheduler's draw stream, a user-facing stream behind `sim.random` /
`sim.uuid4`, and the network's fault stream. They are string-seeded
(`Random(f"{seed}:net")` — stable across processes, since string seeding
hashes via SHA-512 rather than `hash()`).

One shared stream would have been simpler and subtly awful: a program that
draws one extra `sim.random()` value would shift every subsequent scheduling
decision and fault verdict, so unrelated behavior changes would reshuffle
the failure you were chasing. Isolation keeps the blast radius of a change
to its own stream.

`sim` itself (`src/simloop/_sim.py`) is a deliberate concession. Direct
`random` / `uuid` / `time.time()` calls in code under test are honest
nondeterminism simloop cannot intercept without monkeypatching the stdlib —
which was rejected: patching globals leaks across test boundaries and lies
about what production will do. Instead `sim.random` et al. resolve to
seed-derived streams inside a simulation and fall through to the stdlib
outside one, so the same line is deterministic under test and real in
production.

## Fail loudly: the fence policy

Anything that would reach outside the simulation — executors and threads,
signals, subprocesses, raw sockets, `add_reader`/`add_writer`, `getaddrinfo`,
TLS, pipes, `sendfile` — raises `SimulationFenceError` naming the exact call,
and optional stdlib kwargs that would smuggle those in (`ssl=`, `sock=`, …)
are rejected the same way.

The tempting alternative was best-effort passthrough: let `run_in_executor`
actually run things, keep most libraries importable, appear more compatible.
That is the worst possible failure mode for this tool — a harness that
*claims* determinism while real threads race underneath produces
unreproducible "reproducible" failures, and every hour a user spends on a
replay that doesn't replay is trust that never comes back. A loud fence
converts silent wrongness into a documented boundary
([supported-api.md](supported-api.md)) plus an honest error message.

The same posture applies to errors inside the simulation: a run must not
look green while something failed. Unhandled exceptions from fire-and-forget
tasks are collected by the loop's exception handler and re-raised from
`run_until_complete`. One wrinkle earned its own comment in the source: a
failed orphaned task can keep itself alive through a reference cycle (its
exception's traceback pins its own frame), so its failure only reaches the
handler when the cycle collector finalizes it — `run_until_complete` forces
a `gc.collect()` before declaring success so that failure cannot slip past
the boundary.

## The network: packets, not pipes

`SimNetwork` (`src/simloop/_net.py`, `_transports.py`) sits behind the
loop's own `create_connection` / `create_server` /
`create_datagram_endpoint`, so `asyncio.open_connection` and friends work
unchanged. Addresses are host names: `loop.net.host("broker")` declares a
machine, and multi-node systems run as tasks in one process.

The transport layer could have been a pair of byte queues per connection —
far simpler, and how most asyncio test mocks do it. It models packets
instead, with a small TCP-shaped vocabulary (`syn` / `accept` / `refuse` /
`data` / `fin` / `rst`), because the faults worth testing live at packet
granularity: per-packet latency draws, per-datagram drop and duplication,
partitions that catch some packets mid-flight and not others. Byte pipes
cannot express "the acknowledgment was in flight when the partition landed"
— which is precisely the kind of schedule the jobqueue bugs need.

Decisions inside that model, each doing real work:

- **Streams reorder packets, deliver bytes in order.** Independent latency
  draws mean packet N+1 can land before packet N; a per-direction sequence
  number and reassembly buffer restore order. Faithful jitter above, honest
  stream semantics below.
- **Only datagrams are lossy.** A "reliable stream" that loses bytes would
  be lying about being a stream; there is no retransmission model, so stream
  loss under random drop would otherwise wedge every connection. Drop and
  duplication apply to datagram endpoints, where at-most-once is the real
  contract.
- **Partitions blackhole silently.** Datagrams crossing a cut are gone;
  stream packets are *held* and released on heal — with no retransmission,
  permanently dropping a mid-stream packet would leave the receiver waiting
  on a sequence gap forever, so held-then-released is what "the bytes stop,
  then the connection resumes intact" has to mean. Nothing errors: only your
  own timeouts fire, exactly like a real partition.
- **Crashed hosts send no reset.** Their tasks are cancelled, their
  listeners close, and they go silent. Peers cannot distinguish a crash from
  a partition except by timeout — which is the entire epistemology of
  distributed failure detection, enforced by construction.
- **The accept is sequence 0.** The server builds its transport and sends
  `accept` before its protocol's `connection_made` can write; the client
  transport is built when the accept is *dispatched*, not when the connector
  resumes. Data written from `connection_made` (seq 1+) therefore can never
  outrun connection establishment, whatever the latency draws say — an
  ordering race ruled out by construction rather than patched on discovery.
- **Tasks are pinned to hosts through a `ContextVar`**, inherited by every
  task a host's tasks spawn; packet delivery runs the receiving protocol
  under the *destination* host's context, so work spawned from
  `datagram_received` belongs to the receiving machine, and `crash` knows
  exactly which tasks to kill.

What was cut, deliberately: write-side flow control (`drain()` never blocks
— buffers are unbounded), retransmission and congestion modeling, IP
addresses, TLS. Each would deepen the simulation without widening the class
of bugs it can catch; the supported-subset contract beats chasing 100% of
the asyncio surface.

## Explorer and pytest plugin: a thin shell by design

The explorer (`src/simloop/_explore.py`) is deliberately boring: run the
test coroutine on a fresh `SimLoop` per seed, stop at the first failure,
return a report carrying the seed, the trace tail, the trace hash, and every
still-pending task with its await site — the "where was everyone stuck"
snapshot that makes a distributed timeout debuggable. `@sim_test(seeds=N)`
wraps an `async def` into a plain synchronous test; the report is attached
to the original exception as a note, so pytest shows the real traceback
*and* the replay command.

Two structural choices:

- `import simloop` pulls in zero third-party code. The explorer never
  imports pytest; the plugin (`_pytest_plugin.py`, loaded via the `pytest11`
  entry point) feeds `--simloop-seeds` / `--simloop-replay` in through a
  session-scoped overrides object. CLI flags override decorator defaults, so
  CI can crank a 10-seed test to 1,000 without touching code.
- Failure reporting is part of the library, not the plugin, so the same
  report renders anywhere a coroutine can run.

Schedule shrinking — reducing a failing seed's trace to a minimal
interleaving — was considered and deferred; a seed plus an exact replay has
proven sufficient to debug every failure so far.

## What replay actually guarantees

Claimed precisely: **same seed, same code, same interpreter version ⇒
identical execution**, proven by trace-hash equality — across re-runs,
across processes, across hash-randomization seeds. What can still break
replay is *user* code that consults nondeterminism simloop does not control:
direct `random` / `time` / `uuid` calls (use `sim.*`), iteration over sets
of strings (hash-order varies per process unless `PYTHONHASHSEED` is
pinned), and anything fenced that you catch and route around. The fences
make the third category loud; the first two are documented contract. GC
timing is a non-issue for the proof because collection points depend only on
allocation behavior, which identical execution makes identical — and the
suite's cross-process runs would have caught drift there.

## Performance

Numbers and methodology in [benchmarks/](../benchmarks/README.md); the shape
matters more than the digits. A simulated loop never blocks in a selector,
so SimLoop schedules a task step in ~4.4 µs where the stock macOS loop
spends ~16 µs — about half of that inside the per-iteration `kqueue` call —
making the simulation roughly 3.6× faster per step *including* trace
recording. Sleep-heavy workloads compress ~2,000× against wall clock, and
the full jobqueue chaos scenario explores ~55 seeds/second on a laptop.
The trio thread priced deterministic scheduling at ~15% overhead; replacing
the loop instead of instrumenting it turned the overhead negative.

## Case study: jobqueue

`examples/jobqueue/` is the proof of usefulness: an exactly-once job
scheduler (leases, fencing tokens, idempotency keys, backoff, dead-letters)
written in plain stdlib asyncio with no simloop imports, tested entirely
under simulation. Its campaign runs 300 seeds of randomized partitions,
worker crashes, and poison jobs with four invariants checked after every
run; its ablation matrix shows that removing any load-bearing safeguard
yields a seed-replayable violation, while each redundant defense also holds
alone. The full bug table is in
[examples/jobqueue/README.md](../examples/jobqueue/README.md).

The demo also disciplined the harness: carrying a real distributed system
is what shaped the deadlock diagnostics, the pending-task reports, and the
crash and partition semantics described above — nothing exposes a
simulation's blind spots like making it hold up something that matters.

## Future work

Schedule shrinking (greedy trace reduction toward a minimal failing
interleaving); trace diffing between the last passing and first failing
seed; compatibility probes for popular pure-asyncio libraries; a
`sock_*`-level simulation if real demand appears. Each is additive — the
core contract above is meant to stay small and true.
