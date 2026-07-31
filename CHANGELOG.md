# Changelog

## 0.2.0 (unreleased)

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
