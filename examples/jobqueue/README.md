# jobqueue — an exactly-once job scheduler, proven by simloop

A demo distributed system written in plain asyncio (stdlib only, no simloop
imports): one broker, stateless workers, submitting clients. Its test suite
runs entirely under [simloop](../../README.md) — seeded scheduling, virtual
time, simulated partitions and crashes — and every failure it can produce
replays exactly from a seed.

## The claim, stated honestly

Attempts are at-least-once; **effects commit exactly once**. Workers may
re-run a job after a crash or an expired lease, but the fenced, idempotent
effect store accepts one commit per job. No acknowledged job is lost: it
ends `done` (effect committed) or `dead` (dead-lettered after
`max_attempts`, visible, never silent).

Mechanisms: time-based leases with heartbeat renewal, per-job monotonic
fencing tokens checked at both broker and store, idempotency-key submit
dedupe, exponential-backoff requeue, dead-letter state. Time is the only
failure detector — under simloop a crashed peer sends no reset and a
partition stalls silently, exactly like production.

## Invariants

Checked after every simulated run (`tests/invariants.py`):

1. **no-loss** — every acknowledged submit ends done or dead
2. **exactly-once** — at most one accepted commit per job and per logical
   submit; every done job has exactly one
3. **no-zombie-writes** — no commit accepted from a superseded lease
4. **convergence** — nothing left queued or leased at quiesce

## The numbers

- Scenario suite: 7 scenarios × 25–50 seeds each, all green.
- Campaign: **300 seeds** of randomized partitions, a worker crash, and
  poison jobs per seed — invariants held on every seed.
- Ablations: remove any load-bearing safeguard and the explorer finds a
  violating schedule. Found-during-development bugs are listed too.

| # | Safeguard removed / bug | Invariant violated | Found at seed | Seeds searched | Reproduce |
|---|---|---|---|---|---|
| 1 | Store fencing off (`EffectStore(fenced=False)`) | no-zombie-writes | 0 | 1 | `uv run pytest examples/jobqueue/tests/test_mutations.py::test_unfenced_store_admits_a_zombie_write` |
| 2 | Store idempotency off (`EffectStore(idempotent=False)`) | exactly-once | 0 | 1 | `... ::test_unidempotent_store_double_commits_after_a_crash` |
| 3 | Broker fencing off + store ablated | exactly-once | 0 | 1 | `... ::test_broker_fencing_off_lets_a_zombie_finish_the_job` |
| 4 | Client idempotency keys off | exactly-once | 0 | 1 | `... ::test_no_idempotency_key_duplicates_a_lost_ack_submit` |
| 5 | Unbounded retries (`max_attempts=None`) | convergence | 0 | 1 | `... ::test_unbounded_attempts_never_converge_on_poison` |
| 6 | Worker renewals off + store idempotency off | exactly-once | 0 | 1 | `... ::test_renew_off_paired_with_unidempotent_store_double_commits` |

Rows 1–6 are labeled ablations — detection demonstrations, not bugs that
were ever shipped. Renewals off *alone* stays safe across 75 seeds
(defense in depth); so does broker fencing alone — the store is the
last line, and the suite proves both lines independently.

## Run it

    uv run pytest examples/jobqueue/tests -q            # fast suite
    uv run pytest examples/jobqueue/tests -q -m slow    # campaign + safety proofs

Replay any sim-test failure exactly:

    uv run pytest 'examples/jobqueue/tests/test_campaign.py::test_campaign_holds_invariants_under_chaos' -m slow --simloop-replay=0
