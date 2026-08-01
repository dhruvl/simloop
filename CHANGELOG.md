# Changelog

## 0.2.0 (unreleased)

- A second flagship demo: `examples/raft/` is a teaching-sized Raft (leader
  election + log replication, plain asyncio on streams) tested only under
  simulation — four safety invariants checked over 50,000 chaos seeds, five
  safeguard ablations each caught and replayed from a seed, and failing
  schedules minimized toward FIFO — down to a single interesting step in the
  sharpest case.
- Campaign evidence at scale, regenerable via `benchmarks/campaign.py`:
  100,000 seeds of jobqueue chaos green in six minutes on a laptop, every
  ablation caught with its failure density recorded, and 20 sampled failing
  seeds replaying with identical trace hashes across 100 re-runs apiece.
  A small nightly CI sweep keeps the numbers honest.
- Compatibility with third-party libraries is now measured instead of
  claimed: `probes/` drives aiohttp, anyio, websockets, httpx and the Redis
  wire protocol under a SimLoop, and `docs/compatibility.md` publishes what
  each one did, verbatim. Dev-only — the probes are never packaged, their
  pinned dependencies live in their own group, and CI does not run them.
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
  default (traces byte-identical to 0.1.0), with recorded choice lists that
  can replay a schedule independently of its seed (internal, powers
  shrinking).
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
