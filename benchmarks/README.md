# Benchmarks

Three numbers matter for a simulation harness: what the simulated loop costs
per scheduling step, how much simulated time it covers per wall-clock second,
and how fast the explorer burns through seeds on a real test. Measured on a
MacBook Air (Apple M4, 16 GB), macOS, CPython 3.12.13. Every number is the
median of at least 5 runs after one warmup run, with the machine as idle as
a developer laptop gets. Rerun them with the commands below; expect the
ratios, not the absolute times, to
transfer to other machines.

## Scheduling overhead

```
python benchmarks/overhead.py
```

A token circulates around a ring of 100 queue-connected tasks for 200 rounds
(20,000 hops), so the measurement is almost purely task switching and queue
hand-off — no I/O, no timers.

| loop | median | per hop |
|---|---|---|
| stock asyncio | 0.332 s | 16.6 µs |
| SimLoop | 0.090 s | 4.5 µs |

SimLoop comes out about **3.7× faster per scheduling step**, trace recording
included, and the ratio holds from 10×2000 to 500×200 task/round shapes
(0.26–0.31× across the sweep, widening with the task count). That is not
because simloop is a faster event loop in any general sense — it is because a
simulated loop never touches the OS. Profiling the stock run shows about half
its time inside `select.kqueue.control`: the real loop pays a selector
syscall on every iteration even when no I/O is pending, while SimLoop's
iteration is pure Python — pop the PRNG-chosen callback, run it, append a
trace event. The practical reading: replayable scheduling costs nothing at
test time. (For contrast, trio's experimental deterministic-scheduling hook
measured ~15% overhead on top of its normal loop — python-trio/trio#890;
simloop sidesteps the comparison by replacing the loop instead of
instrumenting it.)

The stock-loop baseline is macOS/kqueue; an epoll or io_uring machine will
price the syscall differently.

## Time compression

```
python benchmarks/time_compression.py
```

100 tasks each tick on their own staggered 1–2 s interval, 3600 ticks — the
shape of heartbeats, lease renewals, and retry backoffs. Just under two hours
of simulated time:

| simulated | wall | compression |
|---|---|---|
| 7164 s (1.99 h) | 3.60 s | **~2,000×** |

Virtual time never sleeps: between timers the clock jumps, so a suite full of
`await asyncio.sleep(300)` costs only its callback processing. This is what
makes timeout- and lease-expiry bugs cheap to search for.

## Explorer throughput

```
pytest examples/jobqueue/tests/test_campaign.py -q -m slow
```

The jobqueue chaos campaign runs one full distributed scenario per seed —
a broker, 3 workers, and 2 clients submitting 8 jobs (some poisoned) under
randomized partitions and a worker crash, then settles for up to 600
simulated seconds and checks every invariant. 300 seeds complete in
**4.9–5.4 s** across nine runs (median 5.1 s), about **59 seeds/second**,
in one process. A thousand-seed overnight search is a 17-second coffee
break.

That is on par with — in fact a touch faster than — the ~55 seeds/second
(5.4–6.0 s) published for 0.1.0, even though 0.2.0 records a trace event for
every packet delivery, not just for every send, which is what lets a
timeline draw both ends of a crossing. The extra event volume a
network-heavy scenario like this one pays for no longer shows up as a
wall-clock cost against the 0.1.0 baseline.

## Campaigns

Throughput is only interesting for what it buys: seeds by the hundred
thousand. `benchmarks/campaign.py` spends them three ways, and records what
it finds here.

```
uv run python benchmarks/campaign.py green      [--seeds N] [--jobs J] [--resume]
uv run python benchmarks/campaign.py ablations  [--seeds N] [--jobs J] [--resume]
uv run python benchmarks/campaign.py stability  [--sample K] [--reruns R]
```

`green` sweeps 100,000 seeds of the chaos scenario against the *intact*
jobqueue, in 5,000-seed chunks across `--jobs` processes. Every seed must
pass; a failing one prints its full report and exits nonzero, because an
invariant broken by an intact jobqueue is the most interesting thing this
repository could find.

`ablations` switches one safeguard off at a time — the six mutations
`examples/jobqueue/tests/test_mutations.py` pins — and sweeps 10,000 seeds
each, counting *every* failing seed rather than stopping at the first. That
turns "the explorer catches this bug" into a density: violations per 1,000
seeds. Each failure is checked against the invariant its test claims;
anything else is reported loudly.

`stability` re-runs a sample of those failing seeds 100 times apiece and
requires an identical trace hash every time — replay stability measured at
campaign scale rather than on the handful of seeds the test suite covers.

Both sweeps checkpoint to JSON after every chunk (`--checkpoint FILE`,
default `campaign-{green,ablations}.json`) and resume from it with
`--resume`, so a killed multi-hour run costs one chunk. `stability` reads
the failing seeds out of the `ablations` checkpoint. These files are scratch,
not repository content — they are gitignored.

Results, recorded 2026-08-04 on the M4 MacBook Air (10 jobs):

| campaign | scale | result |
|---|---|---|
| green | 100,000 seeds, 6.3 min, 263.1 seeds/s | green — no invariant violated |
| ablations | 6 mutations × 10,000 seeds, 2.7 min | every ablation caught, densities below |
| replay stability | 20 failing seeds × 100 re-runs, 14.2 s | identical trace hash on every run |

Per-ablation failure density:

| ablation | failures / 10,000 | per 1,000 seeds | first failing seed | invariant |
|---|---|---|---|---|
| unfenced-store | 10,000 | 1000.0 | 0 | no-zombie-writes |
| unidempotent-store | 10,000 | 1000.0 | 0 | exactly-once |
| broker-fencing-off | 10,000 | 1000.0 | 0 | no-zombie-writes |
| no-idempotency-key | 4,953 | 495.3 | 0 | exactly-once, convergence |
| unbounded-attempts | 10,000 | 1000.0 | 0 | TimeoutError |
| renew-off-unidempotent | 10,000 | 1000.0 | 0 | exactly-once |

One thing only scale found: on 2 of the 10,000 `no-idempotency-key` seeds
(3233 and 6475) the duplicate accepted without a key is still queued when
the cluster settles, so the violation surfaces as `convergence` rather than
`exactly-once`. The 200-seed test budget never reaches those seeds; both
flavors are the same missing safeguard, and both replay from their seed
with an identical trace hash.
