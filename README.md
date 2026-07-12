# simloop

Deterministic simulation testing for Python asyncio — seeded task scheduling,
virtual time, and (planned) a simulated network with fault injection. Any failure
it finds replays exactly from a seed.

Rust has [madsim](https://github.com/madsim-rs/madsim) and
[turmoil](https://github.com/tokio-rs/turmoil). Python has nothing comparable.
simloop is that missing tool: it runs **real, unmodified asyncio code** on a
simulated event loop where every scheduling decision is drawn from a seeded PRNG
and time is virtual — so a test that passes 999 times and deadlocks once becomes
a test that deadlocks *every* time at seed 4217, in milliseconds, under a debugger.

## Status

**Early development.** The deterministic loop core works: seeded scheduling,
virtual time, replay-proving trace hashes, deadlock detection, and seeded
`sim.random` / `sim.uuid4` / `sim.time` shims, with the stdlib coordination
primitives (`Queue`, `Event`, `Lock`, `gather`, `TaskGroup`, `timeout`)
running unchanged on top. No simulated network yet. See
[the supported subset](docs/supported-api.md) for the exact contract.

## Planned

- **SimLoop** — a deterministic `asyncio` event loop: virtual clock, seeded
  ready-queue ordering, append-only scheduling trace with replay-proving hashes.
- **SimNetwork** — in-memory transports behind the standard asyncio streams and
  protocol APIs, with seeded fault injection: latency, drops, duplication,
  reordering, and network partitions.
- **pytest plugin** — `@sim_test(seeds=1000)` to explore schedules, and an exact
  replay flag for any failing seed.

Design notes live in [`docs/`](docs/).

## License

[MIT](LICENSE)
