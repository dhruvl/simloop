# Quickstart

From nothing to a replayed failure and a minimized schedule. Every command
and every file on this page is meant to be pasted in order; the outputs are
what they actually printed.

## Install

```
pip install simloop
```

Python 3.12+, no runtime dependencies. The pytest plugin ships inside the
package and registers itself, so there is nothing to enable and nothing to add
to a config file.

## Your first sim test

`@sim_test` turns an `async def` test into an ordinary synchronous test that
pytest collects. There is no fixture, no `asyncio_mode`, and no other asyncio
plugin involved — simloop brings its own loop.

Write `tests/test_ledger.py`:

```python
import asyncio

from simloop import sim_test


@sim_test
async def test_virtual_time_is_free():
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.sleep(300)
    assert loop.time() - start == 300
```

```
pytest tests/test_ledger.py
```

```
tests/test_ledger.py .                                                   [100%]

simloop: 1 sim test, 10 seeds explored
============================== 1 passed in 0.15s ===============================
```

Two things happened there. Five simulated minutes passed in no measurable
wall-clock time, because the clock inside a `SimLoop` only moves when nothing
is left to run. And the test ran ten times, not once: a bare `@sim_test`
explores seeds 0 through 9, each on a fresh loop with a fresh schedule. The
summary line at the end of every run says how many.

## Give it something to race

A deposit and the audit that is supposed to see it, with three background
tasks chattering alongside so the ready queue has choices to make. Add it to
the same file:

```python
@sim_test(seeds=50)
async def test_the_audit_sees_every_deposit():
    loop = asyncio.get_running_loop()
    ledger = {"balance": 0}
    audited = []

    def deposit():
        ledger["balance"] += 1

    def audit():
        audited.append(ledger["balance"])

    async def chatter():
        for _ in range(20):
            await asyncio.sleep(0.001)

    background = [loop.create_task(chatter()) for _ in range(3)]
    await asyncio.sleep(0.005)
    loop.call_soon(deposit)
    loop.call_soon(audit)
    await asyncio.gather(*background)
    assert audited == [1]
```

`seeds=50` asks for fifty schedules instead of ten. Run the file again:

```
pytest tests/test_ledger.py
```

```
E       assert [0] == [1]
E       simloop: failed at seed 2 (2 seeds passed first)
E       replay: pytest 'tests/test_ledger.py::test_the_audit_sees_every_deposit' --simloop-replay=2
E
E       last 20 trace events:
E         [t=0.0200] run      seq=123  driver  _set_result_unless_cancelled
E         [t=0.0200] schedule seq=129  driver  Task.task_wakeup
E         ...
E
E       runs agree for 15 events; passing then ran _set_result_unless_cancelled, failing ran Task.task_wakeup
```

Seeds 0 and 1 passed; seed 2 ran the audit before the deposit it should have
seen. The report is the whole diagnosis: the failing seed, the command that
replays it, the tail of the trace, and a diff against the last passing seed
saying how long the two runs agreed and what each did first at the point they
stopped agreeing.

Searching wider is a command-line option, not a code change:

```
pytest tests/test_ledger.py --simloop-seeds=1000
```

`--simloop-seeds` overrides whatever the decorator asked for, which is how a
suite runs ten seeds on a laptop and a thousand in CI from the same source.

## Replay the failing seed

Paste the replay line the report printed:

```
pytest 'tests/test_ledger.py::test_the_audit_sees_every_deposit' --simloop-replay=2
```

```
E       assert [0] == [1]
E       simloop: failed at seed 2 (0 seeds passed first)
simloop: 1 sim test, 1 seeds explored
1 failed in 0.13s
```

One seed, one run, the same failure. Not "usually the same failure": same
scheduling decisions, same fault decisions, same trace, and the trace hash to
prove it. That is what makes a `--simloop-replay=2` line worth pasting into a
bug report, and it is the point of the whole exercise — a concurrency bug you
can reproduce on demand is a concurrency bug you can debug.

## Shrink the schedule to the race

A failing schedule is mostly noise: hundreds of decisions, of which a handful
mattered. `--simloop-shrink` replays edited copies of the recorded schedule,
walking it back toward plain FIFO order and keeping only the decisions that
still reproduce the failure:

```
pytest 'tests/test_ledger.py::test_the_audit_sees_every_deposit' --simloop-shrink
```

```
E       simloop: failed at seed 2 (2 seeds passed first)
E       schedule shrink (experimental): 137 steps recorded, 14 runs to minimize
E       minimized: FIFO except step 36
E         step 36  test_the_audit_sees_every_deposit.<locals>.audit
```

One step out of 137 had to go a particular way, and it is named: the audit.
When the answer comes back `minimized: FIFO throughout` instead, that is also
an answer — the interleaving never mattered, so look at the fault timings
rather than the task order.

Shrinking is off by default and marked experimental. It costs extra runs of
the workload, capped by `--simloop-shrink-budget` (default 500).

## Draw the run

A schedule reads better as a picture than as a wall of trace lines:

```
pytest tests/test_ledger.py --simloop-timeline=artifacts
```

```
E       timeline: artifacts/simloop-timeline-seed2.html
```

Every failing seed leaves `simloop-timeline-seed<N>.html`, named in its own
report. The page is self-contained — inline CSS, script and SVG, nothing
fetched — so it opens from a CI artifact store as readily as from disk. It
draws one lane per simulated machine and one for the simulation itself,
virtual time running left to right, a dot for every scheduling decision and an
arrow for every packet that crossed.

## Where to go next

- Nothing above touched the network. `loop.net` is where the simulated one
  lives: `set_defaults(latency=..., drop=...)`, `partition` and `heal`,
  `crash` and `restart`, per-host disks that survive a crash, and clocks that
  disagree. The [front page](index.md) has a networked example and the
  [supported API](supported-api.md) has the full contract.
- Property-based testing composes with this without an integration package:
  Hypothesis searches the data, simloop searches the schedule. The recipe, and
  the reasons a seed must not be a strategy, are in the
  [cookbook](cookbook.md).
- Before pointing simloop at a real library, read
  [compatibility](compatibility.md) — what aiohttp, anyio, websockets and httpx
  do under simulation is measured there, not promised.
- Code that bypasses the event loop raises `SimulationFenceError` rather than
  quietly breaking determinism. Which calls those are, and why the line falls
  where it does, is in [supported API](supported-api.md) and
  [design](design.md).
