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

**Early development — nothing usable yet.** First goal: a from-scratch
deterministic event loop that runs a toy client/server pair with identical
trace hashes across repeated runs at the same seed, and different orderings
across seeds.

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
