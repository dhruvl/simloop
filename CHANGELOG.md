# Changelog

## 0.2.0 (unreleased)

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
- A failing seed can draw itself: `--simloop-timeline[=DIR]` writes
  `simloop-timeline-seed<N>.html` for each failing seed and names the file in
  the report, and `simloop.timeline_html(events)` renders any trace the same
  way. One lane per machine and one for the simulation, virtual time left to
  right, a dot per scheduling decision, an arrow for every packet that
  crossed and a stub for every one that did not, with crashes and restarts
  marking the lane they struck. The page is self-contained — inline CSS,
  script and SVG, nothing fetched — and draws the last 5,000 events by
  default, saying so when it dropped any.
- Crashed hosts can come back: `loop.net.restart(name)` (or
  `host.restart()`) revives a machine as a fresh incarnation. It restores
  liveness and nothing else — the old tasks stay cancelled and the
  listeners are gone, so the caller boots the machine again the way it
  booted it the first time. Traffic due while the host was dead is lost,
  leaving peers to notice the outage from their own timeouts. Crashes are
  recorded too: `crash()` writes a trace event and consumes a uid.
- Every host now has `host.disk`, a mapping that survives its crashes:
  where state a real process would fsync belongs. Writes are atomic at
  assignment; there is no partial-write model.
- Clocks can lie per host: `loop.net.set_clock(name, offset=...)` skews
  what that host's tasks read from `loop.time()`, and the deadlines they
  hand to `call_at` with it, while durations (`sleep`, `timeout`,
  `wait_for`, `call_later`) cost the same everywhere — which is what a
  wrong wall clock does to a real machine. Traces stay on the true clock, so
  skew never perturbs scheduling and a run that configures no offset makes
  exactly the decisions it made without the feature.
- A second flagship demo: `examples/raft/` is a teaching-sized Raft (leader
  election + log replication, plain asyncio on streams) tested only under
  simulation — four safety invariants checked over 50,000 chaos seeds, five
  safeguard ablations each caught and replayed from a seed, and failing
  schedules minimized toward FIFO — down to a single interesting step in the
  sharpest case.
- Campaign evidence at scale, regenerable via `benchmarks/campaign.py`:
  100,000 seeds of jobqueue chaos green in under six minutes on a laptop,
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
- `server.sockets` on a simulated server answers with an empty tuple
  instead of not existing, which is all aiohttp's `web.TCPSite` and
  websockets' `serve()` need to start; both now run their documented
  startup paths under simulation.
- Name resolution now stays inside the simulation: every sim host gets a
  stable synthetic IPv4 address (`10.7.0.0/16`, registration order), the
  loop's `getaddrinfo`/`getnameinfo` resolve names and addresses against
  the topology (no real DNS), connections accept either form, and unknown
  names raise `socket.gaierror` deterministically.
- Failure reports now diff the failing seed's schedule against the last
  passing seed's: the report shows how long the two runs agreed, what each
  did at the first disagreement, and a window of context from both traces.
  On by default, costs one retained trace.
- Every scheduling decision flows through a policy seam: seeded draws by
  default, making the same draws the loop made when it owned the PRNG itself,
  with recorded choice lists that can replay a schedule independently of its
  seed (internal, powers shrinking). Policies are shown who is ready, not
  just how many, which is what a priority policy needs.
- Seed exploration can use every core: `explore(fn, seeds, jobs=N)` and
  `--simloop-jobs=N` fan seed batches out over worker processes and report
  exactly what a sequential run would have — the earliest failing seed, with
  its report rebuilt by re-running that seed in the parent, which also
  proves the replay held across processes. Workloads must be picklable to
  cross that boundary, so lambdas, closures and fixture-taking tests stay
  sequential and say so.
- Schedule shrinking, experimental: `--simloop-shrink` (or
  `explore(shrink=True)`) minimizes a failing schedule toward FIFO order,
  keeping only the decisions that reproduce the failure and naming what ran
  at each kept step. Bounded by `--simloop-shrink-budget` (default 500
  extra runs).

## 0.1.0 (2026-07-18)

- Initial release: SimLoop (virtual time, seeded scheduling, trace
  recording), SimNetwork (latency, drops, duplication, partitions, host
  crashes), the seed explorer with `@sim_test` and the pytest plugin
  (`--simloop-seeds`, `--simloop-replay`), and the jobqueue demo.
