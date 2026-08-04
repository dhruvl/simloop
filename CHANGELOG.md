# Changelog

## 0.2.0 (unreleased)

The theme of the release is explanation: a failing seed now diffs itself
against the last passing one, shrinks its schedule to the steps that
mattered, and draws itself as a timeline — and the simulation underneath
grew TLS, real backpressure, disks a crash can tear, restartable hosts and
clocks that lie. Upgrading costs one thing, so it leads the list: every
recorded trace hash moves, once, for every workload.

- Traces now say *where*, and that changes every hash. Each scheduling event
  carries the host it belongs to — the machine that asked for a callback on
  `schedule`, the machine that owns it on `run` and `cancel`, and nothing at
  all for the simulation's own work, like a clock advance or the network's
  delivery step — and every packet that reaches a machine records a `deliver`
  event pairing with its `send` by uid. Both are format changes, so **every
  recorded trace hash differs from the one 0.1.0 produced**, whatever the
  workload does. That subsumes the narrower disclosure this list used to
  carry, when recording crashes was the only thing that moved a hash: there
  is now no workload whose trace is byte-identical to 0.1.0's. What a hash
  means inside a version is untouched — same seed, same code, same
  interpreter, same hash, checked across processes and hash-randomization
  seeds as before.
- `TraceEvent` is a `NamedTuple` rather than a frozen dataclass: one is built
  for every callback the simulation schedules and runs, and a frozen
  dataclass's per-field `object.__setattr__` cost more than twice as much on
  the hottest path in the package. Fields, attribute access and immutability
  are unchanged, and events now unpack and compare as plain tuples — but
  `dataclasses.replace()` and `dataclasses.fields()` on an event raise
  `TypeError` where they used to work.
- Failure reports now diff the failing seed's schedule against the last
  passing seed's: the report shows how long the two runs agreed, what each
  did at the first disagreement, and a window of context from both traces.
  On by default, costs one retained trace.
- Schedule shrinking, experimental: `--simloop-shrink` (or
  `explore(shrink=True)`) minimizes a failing schedule toward FIFO order,
  keeping only the decisions that reproduce the failure and naming what ran
  at each kept step. Bounded by `--simloop-shrink-budget` (default 500
  extra runs).
- A failing seed can draw itself: `--simloop-timeline[=DIR]` writes
  `simloop-timeline-seed<N>.html` for each failing seed and names the file in
  the report, and `simloop.timeline_html(events)` renders any trace the same
  way. One lane per machine and one for the simulation, virtual time left to
  right, a dot per scheduling decision, an arrow for every packet that
  crossed and a stub for every one that did not, with crashes and restarts
  marking the lane they struck. The page is self-contained — inline CSS,
  script and SVG, nothing fetched — and draws the last 5,000 events by
  default, saying so when it dropped any.
- Schedules can be searched by priority instead of by luck:
  `--simloop-policy=pct` (also `explore(policy="pct")` and
  `@sim_test(policy="pct")`) runs Burckhardt et al.'s PCT (ASPLOS 2010) —
  random priorities per chain of work, highest ready one runs, a few randomly
  placed demotions — which buys a stated per-run probability of hitting a bug
  that needs `--simloop-pct-depth` ordering constraints (default 3). The
  horizon those demotions spread over is measured rather than guessed: seed 0
  runs the seeded schedule and its step count sizes it, which costs one extra
  run of the workload and is what lets a found seed replay the schedule that
  found it. PCT explores sequentially, so asking for it alongside
  `--simloop-jobs` is refused rather than quietly ignored. It is a floor, not
  a speed-up — on a shallow race uniform draws reach the bug some 46 times
  sooner, and `docs/supported-api.md` publishes that measurement next to the
  guarantee.
- Seed exploration can use every core: `explore(fn, seeds, jobs=N)` and
  `--simloop-jobs=N` fan seed batches out over worker processes and report
  exactly what a sequential run would have — the earliest failing seed, with
  its report rebuilt by re-running that seed in the parent, which also
  proves the replay held across processes. Workloads must be picklable to
  cross that boundary, so lambdas, closures and fixture-taking tests stay
  sequential and say so. Each worker also freezes its imported heap before
  its first batch, which roughly doubles seeds per second: every run ends
  with a cycle collection — that is what turns a dropped failing task into a
  reported failure — and a full collection otherwise re-walks twenty
  thousand modules and classes a run cannot make garbage. Nothing a run
  builds escapes the collection, because freezing only applies to what
  already exists. Sequential runs are unchanged: the freeze happens in
  simloop's own worker processes, never in yours.
- Every scheduling decision flows through a policy seam: seeded draws by
  default, making the same draws the loop made when it owned the PRNG itself,
  with recorded choice lists that can replay a schedule independently of its
  seed (internal, powers shrinking). Policies are shown who is ready, not
  just how many, which is what a priority policy needs.
- TLS runs inside the simulation. `create_connection(ssl=...)`,
  `create_server(ssl=...)` and `start_tls` on an already-established
  connection all work, and so do `asyncio.open_connection` and `start_server`
  on top of them. The handshake is the real thing — the standard library's
  `SSLProtocol` driving OpenSSL over a pair of memory BIOs, with real
  certificate verification, so a hostname the certificate does not cover
  raises `ssl.SSLCertVerificationError` — and no file descriptor, no real
  socket and no wall-clock second is involved anywhere. Each flight OpenSSL
  produces leaves as one ordinary simulated packet and pays the link's seeded
  latency, so a client connect costs two round trips; `ssl_handshake_timeout`
  and `ssl_shutdown_timeout` are ordinary loop timers, so a handshake a
  partition stalls costs sixty virtual seconds and milliseconds of real ones.
  aiohttp's `https://`, httpx's `https://` and websockets' `wss://` now run
  under simulation, with the evidence in
  [docs/compatibility.md](docs/compatibility.md). The caveat, stated plainly:
  for a workload that uses TLS the hash promise gains two clauses — same
  OpenSSL build, same TLS configuration — because the number of packets a
  handshake makes is a property of the engine. Certificates are not among
  them, measured: an EC leaf and an RSA leaf record the same hash, since the
  trace records how many packets crossed and in what order, never their
  bytes. A run that never asks for TLS is unaffected, which pinned reference
  digests keep true.
- `drain()` can finally block. `loop.net.set_flow_control()` gives stream
  transports a write buffer that holds every byte written but not yet
  received by the peer's protocol — still in flight, held by a partition,
  queued behind an earlier sequence number, or parked because the peer called
  `pause_reading()`. A slow reader, a cut link and a dead peer therefore all
  push back, and a writer paused against a crashed peer keeps waiting until
  its own timeout fires, because a crashed host sends no reset and a real
  sender would wait too. Crossing the high mark calls `pause_writing()` and
  falling back to the low one calls `resume_writing()`, both synchronously,
  which is what turns a backpressure deadlock or an unhandled pause into
  something a seed can find. Off unless you ask for it, and the watermarks
  once armed are the standard library's own `(low=16 KiB, high=64 KiB)`,
  overridable network-wide or per transport with `set_write_buffer_limits`.
  The switch is what makes the defaults safe: `set_write_buffer_limits` on
  its own records numbers without enforcing them, because libraries call it
  uninvited — anyio sets limits on every stream, websockets on every
  connection — and arming on their call would change, or deadlock, workloads
  nobody touched. On hashes: the feature adds no packets and no scheduling
  events of its own, so a run that never arms it is byte-identical to the run
  it was before, checked against digests pinned from before the feature
  existed; an armed run that actually crosses a mark hashes anew, because the
  writer it wakes is a real scheduling decision. One honest divergence from
  TCP: the buffer drains when the peer's *application* receives the bytes,
  with no read-ahead, so simulated backpressure is strictly tighter than the
  real thing — deliberately, since that is what makes a slow consumer
  visibly slow. With the switch off `get_write_buffer_size()` reports `0`,
  which is the truth: nothing is charged and writes leave immediately.
- A host can connect to its own listener. The stream registry used to key
  each connection end by host alone, so the two ends of a self-connection
  collapsed onto one entry and the run deadlocked; the key now carries the
  end's own port, the connect handshake answers to the connector's port
  rather than the listener's, and a loopback connect to a closed port is
  refused instead of hanging. Packets between distinct hosts are keyed,
  ordered and traced exactly as before, so existing hashes do not move.
- `server.sockets` on a simulated server answers with an empty tuple
  instead of not existing, which is all aiohttp's `web.TCPSite` and
  websockets' `serve()` need to start; both now run their documented
  startup paths under simulation.
- Name resolution now stays inside the simulation: every sim host gets a
  stable synthetic IPv4 address (`10.7.0.0/16`, registration order), the
  loop's `getaddrinfo`/`getnameinfo` resolve names and addresses against
  the topology (no real DNS), connections accept either form, and unknown
  names raise `socket.gaierror` deterministically.
- Crashed hosts can come back: `loop.net.restart(name)` (or
  `host.restart()`) revives a machine as a fresh incarnation. It restores
  liveness and nothing else — the old tasks stay cancelled and the
  listeners are gone, so the caller boots the machine again the way it
  booted it the first time. Traffic due while the host was dead is lost,
  leaving peers to notice the outage from their own timeouts. Crashes are
  recorded too: `crash()` writes a trace event and consumes a uid.
- Every host now has `host.disk`, a mapping that survives its crashes:
  where state a real process would fsync belongs. Writes are durable at
  assignment and `disk.sync()` does nothing — until the host asks for
  otherwise with `loop.net.set_disk(name, buffered=True)`, which queues
  writes and deletes until a `sync()` while the host itself reads them
  back immediately. A crash then takes the queue with it, and `torn=True`
  keeps a seeded prefix of it instead: the state a machine that lost power
  mid-batch actually reboots into. A prefix is all it claims to be —
  nothing is reordered and no value is ever half-written. A disk nobody
  configured is untouched down to the draw: storage records no trace
  events and consumes no randomness, so those runs decide everything
  exactly as they did before.
- Clocks can lie per host: `loop.net.set_clock(name, offset=...)` skews
  what that host's tasks read from `loop.time()`, and the deadlines they
  hand to `call_at` with it, while durations (`sleep`, `timeout`,
  `wait_for`, `call_later`) cost the same everywhere — which is what a
  wrong wall clock does to a real machine. Traces stay on the true clock, so
  skew never perturbs scheduling and a run that configures no offset makes
  exactly the decisions it made without the feature.
- Executor submissions run inline instead of fencing: `loop.run_in_executor`
  executes the function at an ordinary scheduled step — ordered by the
  seeded draw, labelled `executor:<function>` in the trace, costing no
  virtual time — and its result or exception lands on the returned future
  the way a worker would land it, so `asyncio.to_thread` works under
  simulation. The executor argument is never used (there is no pool and
  nothing runs concurrently), and `set_default_executor` still fences: a
  pool that would never run anything is refused rather than accepted.
  `call_soon_threadsafe` from the loop's own thread is now `call_soon`,
  which is all it ever was without a second thread; from any other thread
  it still fences. None of this reaches `anyio.to_thread`, whose worker
  threads are real ones spawned through no loop API — a real thread racing
  a virtual clock ends in the cross-thread fence, a hang, or the caller's
  own timeout, whichever the race picks.
- A second flagship demo: `examples/raft/` is a teaching-sized Raft (leader
  election + log replication, plain asyncio on streams) tested only under
  simulation — four safety invariants checked over 50,000 chaos seeds, six
  safeguard ablations each caught and replayed from a seed, and failing
  schedules minimized toward FIFO — down to a single interesting step in the
  sharpest case. Its state can live on host disks that buffer and tear, with
  the syncs Raft owes before it answers an RPC; drop the sync before an
  append is acknowledged and the first seed loses a committed entry.
- Campaign evidence at scale, regenerable via `benchmarks/campaign.py`:
  100,000 seeds of jobqueue chaos green in just over four minutes on a laptop,
  every ablation caught with its failure density recorded, and 20 sampled
  failing seeds replaying with identical trace hashes across 100 re-runs
  apiece. A small nightly CI sweep keeps the numbers honest.
- Compatibility with third-party libraries is now measured instead of
  claimed: `probes/` drives aiohttp, anyio, websockets, httpx and the Redis
  wire protocol under a SimLoop, and `docs/compatibility.md` publishes what
  each one did, verbatim. Dev-only — the probes are never packaged, their
  pinned dependencies live in their own group, and CI does not run them.
- Composing with Hypothesis is a documented recipe with a test behind it:
  `docs/cookbook.md` walks through `@given` generating the workload while
  `explore()` runs it under a range of seeds, and
  `tests/test_hypothesis_recipe.py` runs that composition in CI — a green case
  across examples and seeds, and a planted bug where Hypothesis shrinks the
  workload to its minimum while the reported seed replays the schedule on its
  own. It is a recipe rather than an integration: nothing was added to the
  package, seeds are deliberately not a strategy (a seed has no size to shrink
  toward, and two shrinkers aimed at one failure fight), and Hypothesis is a
  dev dependency of this repository rather than something simloop imports.

## 0.1.0 (2026-07-18)

- Initial release: SimLoop (virtual time, seeded scheduling, trace
  recording), SimNetwork (latency, drops, duplication, partitions, host
  crashes), the seed explorer with `@sim_test` and the pytest plugin
  (`--simloop-seeds`, `--simloop-replay`), and the jobqueue demo.
