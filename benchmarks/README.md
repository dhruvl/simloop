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
hand-off, with no I/O and no timers. The loops take turns, nine runs each
after a warmup, and the script reports wall clock and CPU time side by side.
With uvloop importable it adds a row
(`uv run --with uvloop python benchmarks/overhead.py`); simloop does not
depend on it.

| loop | wall per hop, median (range) | CPU per hop, median (range) |
|---|---|---|
| stock asyncio | 16.67 µs (16.22–16.78) | 5.18 µs (4.67–5.24) |
| uvloop 0.22.1 | 14.93 µs (14.86–15.80) | 3.44 µs (3.38–4.29) |
| SimLoop | 4.51 µs (4.43–4.58) | 4.49 µs (4.43–4.58) |

Measured 2026-09-29 on a quiet machine. Two earlier sittings that day, with a
system update running, agreed within 0.3 µs on every median, and the stock
loop's wall figure matches the 16.6 µs recorded in August.

The two columns disagree, and the disagreement is the finding. By wall clock
SimLoop finishes the ring in under a third of the time either real loop
takes. By CPU time it costs about what the stock loop costs, and uvloop is
cheaper than both. The real loops spend roughly two-thirds of their wall time
off the CPU, waiting in `select.kqueue.control` on every iteration even when
nothing is pending; SimLoop never makes that call, so its wall time and CPU
time are the same number. None of that makes SimLoop a faster event loop;
uvloop does less work per step than either. The practical reading is
narrower: replayable scheduling, trace recording included, costs nothing at
test time. (For contrast, trio's experimental deterministic-scheduling hook
measured ~15% overhead on top of its normal loop, python-trio/trio#890;
simloop sidesteps the comparison by replacing the loop instead of
instrumenting it.)

The August run swept task/round shapes from 10×2000 to 500×200 and found the
wall-clock ratio between 0.26× and 0.31× throughout, widening with the task
count. The baseline is macOS/kqueue; an epoll or io_uring machine will price
the wait differently, and the CPU column is the one to expect to transfer.

## Time compression

```
python benchmarks/time_compression.py
```

100 tasks each tick on their own staggered 1–2 s interval, 3600 ticks, the
shape of heartbeats, lease renewals, and retry backoffs. Just under two hours
of simulated time:

| simulated | wall, median (range) | compression, median (range) |
|---|---|---|
| 7164 s (1.99 h) | 3.61 s (3.51–4.04) | **~2,000×** (1,775–2,040×) |

Nine runs, measured 2026-09-29 alongside the table above.

Virtual time never sleeps: between timers the clock jumps, so a suite full of
`await asyncio.sleep(300)` costs only its callback processing. This is what
makes timeout- and lease-expiry bugs cheap to search for.

## Explorer throughput

```
pytest examples/jobqueue/tests/test_campaign.py -q -m slow
```

The jobqueue chaos campaign runs one full distributed scenario per seed:
a broker, 3 workers, and 2 clients submitting 8 jobs (some poisoned) under
randomized partitions and a worker crash, then settles for up to 600
simulated seconds and checks every invariant. 300 seeds complete in
**4.9–5.4 s** across nine runs (median 5.1 s), about **59 seeds/second**,
in one process, as recorded on 2026-08-04; the 2026-09-29 sitting under
Campaigns took 5.2–6.3 s. A thousand-seed overnight search is a 17-second coffee
break.

That is a touch faster than the ~55 seeds/second
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

`ablations` switches one safeguard off at a time (the six mutations
`examples/jobqueue/tests/test_mutations.py` pins) and sweeps 10,000 seeds
each, counting *every* failing seed rather than stopping at the first. That
turns "the explorer catches this bug" into a density: violations per 1,000
seeds. Each failure is checked against the invariant its test claims;
anything else is reported loudly.

`stability` re-runs a sample of those failing seeds 100 times apiece and
requires an identical trace hash every time, which measures replay stability at
campaign scale rather than on the handful of seeds the test suite covers.

Both sweeps checkpoint to JSON after every chunk (`--checkpoint FILE`,
default `campaign-{green,ablations}.json`) and resume from it with
`--resume`, so a killed multi-hour run costs one chunk. `stability` reads
the failing seeds out of the `ablations` checkpoint. These files are scratch,
not repository content, and they are gitignored.

Results on the M4 MacBook Air (10 jobs). The green row was re-run three
times on 2026-09-29; the other two were recorded on 2026-08-04:

| campaign | scale | result |
|---|---|---|
| green | 100,000 seeds × 3 runs, 3.5–3.8 min each, 443.3–472.6 seeds/s (median 472.5) | green, no invariant violated |
| ablations | 6 mutations × 10,000 seeds, 3.0 min | every ablation caught, densities below |
| replay stability | 20 failing seeds × 100 re-runs, 15.7 s | identical trace hash on every run |

Ten processes do not give ten times one process. In the same sitting as the
three green runs, the single-process campaign above took 5.2–6.3 s for 300
seeds (median 5.5 s, about 55 seeds/s), so ten workers delivered about 8.7
times one: roughly 47 seeds/s each. This M4 has four performance cores and
six efficiency cores, so six of the ten workers run on the slower kind,
which is the likeliest reason.

The single run recorded on 2026-08-04 made 394.5 seeds/s, and no library
code changed between the two dates. That is the spread the next section is
about: the same sweep on the same laptop, 20% apart.

The green row is where the workers' frozen heap shows up: nearly every green
seed reaches the settle phase and pays the end-of-run collections in full,
so taking the imported heap out of them moved this number from 263 to 394
seeds/s in one sitting on this machine. The ablation runs barely moved,
because most of those seeds fail within a few steps and never accumulate the work
the collection was re-walking.

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

## What the green number measures

That throughput says as much about the laptop as about the code, which is
worth stating next to it. The same sweep recorded 300.1 seeds/s on
2026-08-01 and 263.1 on 2026-08-04, about 12% apart, with four merges in
between: the inline executor, loopback streams, write-side flow control, and
TLS. None of them is what paid.

Wall clock cannot settle that question on a fanless laptop. Five interleaved
2,500-seed runs of each of those merge commits spread 230–265 seeds/s
*within* every commit, a swing as wide as the drift being chased. CPU time
can: summed across the ten workers it does not care how busy the machine is,
and by that measure a seed costs the same across the whole window.

| tree | seeds / CPU-second | vs #24 |
|---|---|---|
| #24 disk-sync, where 300.1 was measured | 32.77 | baseline |
| #25 inline executor | 32.82 | +0.2% |
| #26 loopback streams | 32.74 | −0.1% |
| #27 write-side flow control | 32.79 | +0.1% |
| #28 TLS | 32.62 | −0.5% |
| main at 0.2.0 | 32.49 | −0.8% |

Median of five interleaved runs each, 2,500 seeds, 10 jobs, with every
tree's own spread under 1%. The single-process sweep agrees: 63.7 to 63.9
seeds/CPU-second over the same four merges. Running the sweep itself back to
back closes the question: on that day main covered 20,000 seeds at 262.8
seeds/s, reproducing the recorded 263.1, while the #24 tree that recorded
300.1 managed 259.2 in the same sitting. The 300.1 was a quieter machine, not
faster code.

Two of those merges do touch the per-packet path, so their costing nothing
is the part worth recording. Flow control charges a write and credits a
delivery; with the switch off each is an attribute read and a return, about
150 calls a seed and 0.1% of profiled time. Loopback streams widened the
stream registry key from a 2-tuple to a 3-tuple, one more element to hash
per dispatch. Neither is visible against where a seed's time actually goes:
**38.6%** of it is the `gc.collect()` that `run_until_complete` needs in
order to surface an orphaned task's failure, at 2.4 ms a call, twice per
seed, once for the run and once for the teardown drain.

So: re-measure back to back against whichever tree the number is being
compared with, on a machine doing nothing else. Ratios taken in one sitting
transfer; absolute numbers recorded three days apart do not.
