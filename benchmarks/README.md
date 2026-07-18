# Benchmarks

Three numbers matter for a simulation harness: what the simulated loop costs
per scheduling step, how much simulated time it covers per wall-clock second,
and how fast the explorer burns through seeds on a real test. Measured on a
MacBook Air (Apple M4, 16 GB), macOS, CPython 3.12.13. Every number is the
median of 5 runs after one warmup run, on an otherwise idle machine. Rerun
them with the commands below; expect the ratios, not the absolute times, to
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
| stock asyncio | 0.319 s | 16.0 µs |
| SimLoop | 0.088 s | 4.4 µs |

SimLoop comes out about **3.6× faster per scheduling step**, trace recording
included, and the ratio holds from 10×2000 to 500×200 task/round shapes
(0.26–0.29× across the sweep). That is not because simloop is a faster event
loop in any general sense — it is because a simulated loop never touches the
OS. Profiling the stock run shows about half its time inside
`select.kqueue.control`: the real loop pays a selector syscall on every
iteration even when no I/O is pending, while SimLoop's iteration is pure
Python — pop the PRNG-chosen callback, run it, append a trace event. The
practical reading: replayable scheduling costs nothing at test time. (For
contrast, trio's experimental deterministic-scheduling hook measured ~15%
overhead on top of its normal loop — python-trio/trio#890; simloop sidesteps
the comparison by replacing the loop instead of instrumenting it.)

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
| 7164 s (1.99 h) | 3.55 s | **~2,000×** |

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
**5.4–6.0 s**, about **55 seeds/second**. A thousand-seed overnight search is
a 20-second coffee break.
