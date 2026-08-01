# raft — leader election and log replication, proven by simloop

A teaching-sized Raft: about 500 lines of plain asyncio (stdlib only, no
simloop imports) covering leader election, log replication, persistence
across process restarts, and an applied state machine. Its test suite runs
entirely under [simloop](../../README.md) — seeded scheduling, virtual time,
simulated partitions, process restarts, message loss and duplication — and
every failure it can produce replays exactly from a seed.

**A demo, not a library.** No log compaction and no snapshots, no membership
changes, no client sessions. Submission is at-least-once and says so: a
command a leader accepts before being deposed can be resubmitted and commit
twice, under two indices, and the safety claims below deliberately do not
mind. Storage is an in-memory stand-in that survives a process restart within
a run, the way a disk survives a reboot.

## The claim, stated honestly

The paper's four safety properties hold under every schedule the explorer
reaches. Liveness is not claimed: a partitioned minority makes no progress,
and the suite waits in virtual time rather than asserting any bound.

Each rule that carries the safety argument sits behind its own flag in
`Safeguards`, so the tests can switch exactly one off and watch the explorer
find the schedule it lets through: stale-term rejection, one vote per term
(§5.2), the log-freshness check on votes (§5.4.1), persistence before reply,
the own-term commit gate (§5.4.2), and the per-term no-op that lets a quiet
term commit (§8).

Time is the only failure detector. Under simloop a partition stalls silently
and a restarted process sends no reset, so a peer learns of trouble the way it
would in production: from a request that never comes back.

## Invariants

Checked after every simulated run (`tests/checks.py`) against an ordered
record of what the nodes observably did — `("leader", name, term, log)` at
each election, `("apply", name, index, term, command)` at each state-machine
apply — plus the logs the run ends with:

1. **election safety** — at most one leader per term
2. **log matching** — if two logs hold the same term at an index, they hold
   the same prefix before it
3. **leader completeness** — an entry committed under a term-T leader is in
   the log of every leader of a term above T
4. **state-machine safety** — no index is ever applied as two different
   entries

## The numbers

- Scenario suite: 11 seeded scenarios (elections, replication, RPC framing)
  × 10 seeds each, alongside unit tests for the log, vote and persistence
  rules — 66 tests in all, green.
- Campaign: **50,000 seeds** of five-node chaos — three randomized partition
  windows per seed, a process restart after about half of them, 2% message
  drop and 2% duplication throughout, with a client proposing — invariants
  held on every seed. 917.29s (15m17s) with `--simloop-jobs=8` on an M4
  MacBook Air, about 54 seeds a second. The same scenario runs 300 seeds
  sequentially in 27.97s and 2,000 seeds in 36.34s at `--simloop-jobs=8`.
- Replay stability: each ablation's found-at seed re-explored 100 times on a
  fresh loop — 5 seeds × 100 replays, one trace hash apiece, byte-identical
  throughout. The suite keeps re-earning the property:
  `test_a_found_seed_replays_byte_identically` (slow-marked) finds a failing
  seed and holds its trace hash across eight fresh replays.
- Ablations: remove any load-bearing safeguard and the explorer finds a
  violating schedule within a few seeds.

| # | Safeguard removed | Invariant violated | Found at seed | Seeds searched | Reproduce |
|---|---|---|---|---|---|
| 1 | Vote ledger off (`one_vote_per_term=False`) | election-safety | 2 | 3 | `uv run pytest examples/raft/tests/test_ablations.py::test_double_voting_elects_two_leaders_in_one_term` |
| 2 | Log-freshness check off (`check_log_up_to_date=False`) | leader-completeness | 0 | 1 | `... ::test_unchecked_logs_let_a_stale_follower_lead` |
| 3 | Persistence before reply off (`persist_before_reply=False`) | leader-completeness | 4 | 5 | `... ::test_skipped_persistence_forgets_committed_entries` |
| 4 | Stale-term rejection off (`reject_stale_term=False`) | state-machine-safety | 0 | 1 | `... ::test_accepting_stale_terms_rewrites_history` |
| 5 | Commit gate off (`commit_own_term_only=False`, no-op off both sides) | state-machine-safety | 3 | 4 | `... ::test_committing_old_terms_by_count_loses_writes` |

Rows 1–4 searched a budget of 300 seeds, row 5 a budget of 500. All five are
labeled ablations — detection demonstrations, not bugs that were ever
shipped.

Two safeguards are also shown to be load-bearing *on their own*, which is the
other half of the argument: with the commit gate the only thing standing (the
per-term no-op switched off on both sides), the paper's Figure 8 runs clean
across 150 seeds; with the vote ledger the only thing standing, the
double-election scenario runs clean across 150 seeds. Both proofs are marked
`slow`.

### Found during development

The replay-stability check above is what caught the last bug. `RaftNode.run`
tracked its in-flight connection handlers in a `set` and cancelled them in
iteration order when the incarnation died — but a set of tasks iterates by
`id()`, which moves between runs, so two handlers could swap places. Seed 4
of the persistence ablation produced two distinct trace hashes across 100
re-runs: the same violation every time, reached by two different routes.
An insertion-ordered dict fixed it. Nothing about the simulation was wrong:
the demo was letting `id()` order decide something the schedule could see,
which is exactly what the third rule in
[docs/design.md](../../docs/design.md) tells the loop's own structures never
to do. The found-at seeds in the table are unchanged either side of the fix.

## What a failure looks like

Each ablation test asserts that the explorer catches its ablation, so the
tests themselves pass; the report is what the explorer hands back on the way.
It names the invariant, the seed, and the trace around the failure. Run the
same scenario through `explore(scenario, range(300), shrink=True)` and it
also walks the recorded schedule back toward plain FIFO order, keeping only
the decisions that have to go a particular way for the failure to reproduce.
Stale-term rejection off, seed 0:

    schedule shrink (experimental): 3,514 steps recorded, 57 runs to minimize
    minimized: FIFO except step 1
      step 1  TaskStepMethWrapper

3,514 recorded scheduling steps; exactly one of them had to go a specific
way. `FIFO throughout` is an answer too, and the double-voting ablation gives
it across 1,169 steps — nothing about the task order matters there, so that
race lives in the fault timing, in where the partition falls relative to the
election timeouts.

Re-run any detection from the table:

    uv run pytest examples/raft/tests/test_ablations.py::test_accepting_stale_terms_rewrites_history

Shrinking is experimental and costs extra runs; the `--simloop-shrink` flag
reaches `@sim_test` tests, while the ablations call `explore()` directly and
take `shrink=True` as an argument.

## Run it

    uv run pytest examples/raft/tests -q            # fast suite: scenarios, units, ablations
    uv run pytest examples/raft/tests -q -m slow    # chaos campaign + the two safe proofs

Turn the campaign up and spread the seeds over cores:

    uv run pytest examples/raft/tests/test_chaos_campaign.py -q -m slow --simloop-seeds=50000 --simloop-jobs=8

Replay any campaign failure exactly:

    uv run pytest 'examples/raft/tests/test_chaos_campaign.py::test_chaos_campaign_holds_the_invariants' -m slow --simloop-replay=0
