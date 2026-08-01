# Cookbook

Recipes that are known to work, because each one is also a test in this
repository. One so far.

## Hypothesis for the data, simloop for the schedule

A concurrency bug usually needs two things to go wrong at once: a workload
that can race — enough clients, the right payloads, a timeout short enough to
matter — and an interleaving that makes it race. Property-based testing and
simulation testing each search one of those and neither searches the other.
[Hypothesis](https://hypothesis.readthedocs.io/) generates and minimizes
*data*; simloop enumerates *schedules* and replays the one that failed.

They compose without an integration layer. Hypothesis picks the workload,
`explore()` runs that workload under a range of seeds, and the property is
"no seed broke it". There is no simloop-Hypothesis package to install and
nothing in simloop knows Hypothesis exists — the whole recipe is the shape of
one test function, which is why this is a cookbook page rather than a module.

The worked example below is
[tests/test_hypothesis_recipe.py](https://github.com/dhruvl/simloop/blob/main/tests/test_hypothesis_recipe.py),
which runs in simloop's CI on every commit.

### The workload

Writers appending to one shared log, where appending is "read the length,
then write at that index" — with an `await` in the gap, the way a real write
has network or disk in the middle:

```python
async def reserve_then_write(log, payload, delay):
    index = len(log)
    await asyncio.sleep(delay)
    log[index : index + 1] = [payload]


async def replicate(writers, payloads, delay, *, guarded):
    loop = asyncio.get_running_loop()
    lock = asyncio.Lock()
    log = []

    async def write_batch():
        for payload in payloads:
            if guarded:
                async with lock:
                    await reserve_then_write(log, payload, delay)
            else:
                await reserve_then_write(log, payload, delay)

    await asyncio.gather(*[loop.create_task(write_batch()) for _ in range(writers)])
    expected = writers * len(payloads)
    assert len(log) == expected, f"lost {expected - len(log)} of {expected} appends"
```

Three parameters — how many writers, what they write, how long a write takes
in virtual seconds — and one invariant: every append that started is in the
log. `guarded=True` holds the lock across the reserve and the write, which is
the fix.

### The recipe

```python
from hypothesis import given, settings
from hypothesis import strategies as st

from simloop import explore

SEEDS = 8


@settings(deadline=None, derandomize=True, database=None, max_examples=25)
@given(
    writers=st.integers(min_value=1, max_value=4),
    payloads=st.lists(st.text(alphabet="abc", min_size=1, max_size=3),
                      min_size=1, max_size=3),
    delay=st.sampled_from((0.0, 0.001, 0.010)),
)
def test_the_log_keeps_every_append(writers, payloads, delay):
    report = explore(
        lambda: replicate(writers, payloads, delay, guarded=True), range(SEEDS)
    )
    assert report is None, report.render()
```

Read it as one sentence: for every workload Hypothesis can build, none of the
first `SEEDS` schedules loses an append. `explore()` returns a
[`SeedReport`](supported-api.md#exploring-schedules) for the first seed that
failed and `None` when they all passed, so the property is a plain `is None`
and the report — failing seed, replay command, trace tail, schedule diff — is
the assertion message.

Drop the lock (`guarded=False`) and the combination finds the bug, at which
point the two searches divide the reproduction between them.

### The short form

`@sim_test` and `@given` stack, and the arguments flow through:

```python
@settings(deadline=None, derandomize=True, database=None, max_examples=25)
@given(writers=st.integers(min_value=1, max_value=4))
@sim_test(seeds=8)
async def test_the_log_keeps_every_append(writers):
    await replicate(writers, ["a"], 0.0, guarded=True)
```

`@sim_test` turns the coroutine into a synchronous test that explores seeds
and re-raises the first failure with the report attached; `@given` calls that
test once per example. Use this form when you want the pytest options
(`--simloop-seeds`, `--simloop-replay`, `--simloop-shrink`,
`--simloop-timeline`) to reach the exploration; use the explicit `explore()`
form when you want the report as a value — to assert on the failing seed, or
to keep exploring after one.

One option does not survive the stack: `--simloop-jobs` refuses any test that
takes arguments, because worker processes cannot rebuild them. Its message
talks about fixtures; a Hypothesis argument is the same problem.

### What a failure gives you

Both halves land in the same output. Hypothesis prints the minimal workload
it could still fail with, and simloop's report names the seed and the command
that replays it:

```
E       AssertionError: lost 1 of 2 appends
E       assert 1 == 2
E        +  where 1 = len(['a'])
E       simloop: failed at seed 0 (0 seeds passed first)
E       replay: pytest 'tests/test_log.py::test_the_log_keeps_every_append' --simloop-replay=0
E
E       last 20 trace events:
E         [t=0.0000] run      seq=2  driver  TaskStepMethWrapper
E         ...
E       Failing test case: test_the_log_keeps_every_append(
E           writers=2,
E       )
```

Two writers is the smallest workload that can lose an append, and seed 0 is a
schedule where it does. Nothing about that pair is approximate: rerun the same
workload at that one seed and you get the same failure, the same trace and the
same trace hash.

The one thing to know when you turn this into a regression test:
`--simloop-replay=SEED` pins the schedule, not the example. To pin both, write
the minimized workload down as an explicit case — `@example(writers=2)`, or a
plain non-Hypothesis test calling `explore()` with those arguments — and keep
the property test for the search.

### Why the seed is not a strategy

The obvious next move is `seed=st.integers()`, and it is a mistake. It puts
both searches inside one shrinker, and they minimize incompatible things.

- **A seed has no size.** Hypothesis shrinks toward smaller values because
  smaller usually means simpler, and for a seed it means nothing at all: a
  seed is an opaque index into the space of schedules, so seed 0 is not a
  simpler failure than seed 8,172 — it is a different one, and usually one
  that does not reproduce. The shrinker spends its budget wandering between
  unrelated schedules instead of cutting down the workload.
- **The property stops being a function.** With a fixed seed range, "this
  workload fails" is deterministic — the same arguments always produce the
  same verdict. Draw the seeds too and the same workload passes or fails
  depending on what was drawn, which is exactly the flakiness Hypothesis
  cannot shrink through: it will abandon a shrink it cannot reproduce and say
  so.
- **The schedule already has its own shrinker, and it works on the right
  object.** Minimizing an interleaving is not minimizing a number; it is
  editing the recorded scheduling decisions back toward FIFO order and
  keeping the ones that matter. That is `--simloop-shrink`, and it runs on
  the failing seed after the fact.

So: Hypothesis owns the data, `range(SEEDS)` owns the schedules, and the two
shrinkers never meet. If a workload needs more schedule coverage, raise
`SEEDS` (or `--simloop-seeds` in CI) — that is a knob, not a search space.

### Settings that matter

`deadline=None` is not optional. Hypothesis's per-example deadline (200 ms by
default) is a wall-clock measurement, and under simulation wall-clock time is
not the thing being tested: a virtual `asyncio.sleep(300)` costs nothing, one
example is many simulated runs, and how long they take is a statement about
the machine. On a busy CI runner the deadline fires as a flaky failure with
nothing behind it.

`derandomize=True` and `database=None` are what make the suite reproducible.
Derandomizing derives the examples from a hash of the test, so every run tests
the same workloads until the code or the version changes; `database=None`
stops Hypothesis from replaying examples out of a local `.hypothesis/`
directory that CI does not have. Together with the fixed seed range, the test
is then a pure function of the repository: a green run means something, and a
red one reproduces on the first try.

That is the CI story, not the only story. Locally, the database is worth
having — it remembers the workload that failed and tries it first next time —
and dropping `derandomize` widens the search across runs. Both are reasonable
in a nightly sweep. Neither belongs in a test that is supposed to give the
same answer on every machine.

### What it costs

One example is one full exploration, so the workload runs `max_examples ×
SEEDS` times. The numbers above are 25 × 8 = 200 runs, which is a couple of
seconds for a workload this size because virtual time is free. It multiplies,
though: 500 examples over 1,000 seeds is half a million runs, so raise the two
knobs deliberately and separately. More examples buys workload variety; more
seeds buys schedule coverage for the workloads you already have.

### Honest limits

- The minimal example belongs to Hypothesis, and which minimum it reports can
  change when Hypothesis does. simloop's own test asserts on the exact
  shrunk workload on purpose — that assertion is what proves the shrinking is
  real — and the version is pinned in `uv.lock`. Asserting on a minimum is a
  choice to make knowingly, not a default.
- This page covers `@given`. Hypothesis's stateful testing drives its own
  run loop, and nothing here says what a `RuleBasedStateMachine` does on top
  of a simulated one; it is untried rather than known to work.
- Hypothesis is not a dependency of simloop and never will be under this
  recipe. It is a dev dependency of this repository, so that the recipe can
  be tested.
