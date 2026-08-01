import asyncio
import gc
import random
import weakref
from collections.abc import Callable, Sequence

import pytest

from simloop import Host, SimLoop
from simloop._policy import (
    MAX_CHOICE,
    PCTPolicy,
    ReadyView,
    SchedulingPolicy,
    ScriptedPolicy,
    SeededPolicy,
)


def _ready(count: int) -> list[ReadyView]:
    """A ready queue of ``count`` views, contents deliberately arbitrary.

    The count-only policies must not read owners or labels, so the values
    here are noise: any policy that looks at them is looking at nonsense.
    """
    return [(-1 - index, f"callback{index}") for index in range(count)]


def test_seeded_policy_matches_a_bare_random_stream() -> None:
    # The seam only holds if the policy draws exactly what the loop used to
    # draw: same seed, same calls, same values, in the same order.
    sizes = [1, 2, 3, 7, 2, 64, 5, 1, 12, 3, 9]
    reference = random.Random(41)
    policy = SeededPolicy(41)
    assert [policy.choose(_ready(n)) for n in sizes] == [
        reference.randrange(n) for n in sizes
    ]


def test_seeded_policy_ignores_everything_but_the_ready_count() -> None:
    # The property the whole schedule-identity guarantee rests on: widening
    # the protocol gave the default policy more to look at, and it looks at
    # none of it. Same lengths, wildly different views, identical draws.
    sizes = [3, 1, 8, 2, 5, 5, 13]
    plain = SeededPolicy(41)
    exotic = SeededPolicy(41)
    assert [plain.choose(_ready(n)) for n in sizes] == [
        exotic.choose([(7, "Task.task_wakeup")] * n) for n in sizes
    ]


def test_seeded_policy_never_diverges() -> None:
    policy = SeededPolicy(0)
    for _ in range(10):
        policy.choose(_ready(4))
    assert policy.diverged_at is None


def test_scripted_policy_replays_its_recording() -> None:
    policy = ScriptedPolicy([2, 0, 1])
    assert [
        policy.choose(_ready(3)),
        policy.choose(_ready(4)),
        policy.choose(_ready(2)),
    ] == [2, 0, 1]
    assert policy.diverged_at is None


def test_scripted_policy_clamps_choices_past_the_ready_queue() -> None:
    policy = ScriptedPolicy([5, 1])
    assert policy.choose(_ready(3)) == 2
    assert policy.diverged_at == 0
    assert policy.choose(_ready(2)) == 1


def test_scripted_policy_marks_only_the_first_divergence() -> None:
    policy = ScriptedPolicy([0, 9, 9])
    assert [
        policy.choose(_ready(4)),
        policy.choose(_ready(2)),
        policy.choose(_ready(2)),
    ] == [0, 1, 1]
    assert policy.diverged_at == 1


def test_exhausted_scripted_policy_falls_back_to_fifo() -> None:
    policy = ScriptedPolicy([1])
    assert policy.choose(_ready(3)) == 1
    assert policy.diverged_at is None
    assert [policy.choose(_ready(3)), policy.choose(_ready(3))] == [0, 0]
    assert policy.diverged_at == 1


def test_empty_recording_is_pure_fifo() -> None:
    policy = ScriptedPolicy([])
    assert policy.choose(_ready(5)) == 0
    assert policy.diverged_at == 0


def test_scripted_policy_rejects_impossible_choices() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        ScriptedPolicy([0, -1])
    with pytest.raises(ValueError, match="too large"):
        ScriptedPolicy([MAX_CHOICE + 1])


# ----------------------------------------------------------------------
# Priority-based exploration
# ----------------------------------------------------------------------


def _owned(*owners: int) -> list[ReadyView]:
    """A ready queue owned by ``owners``, in that order."""
    return [(owner, f"task{owner}.step") for owner in owners]


# Queue shapes with owners appearing, vanishing and coming back, so a run
# over them exercises more than one queue length and more than one position.
_SCRIPT = [
    _owned(0, 1, 2),
    _owned(1, 2),
    _owned(0, 2, 3, 4),
    _owned(4, 0),
    _owned(2, 3, 0, 1),
    _owned(5),
    _owned(5, 1, 0),
    _owned(3, 4, 5, 0, 1, 2),
]


def _drive(policy: PCTPolicy) -> list[int]:
    return [policy.choose(views) for views in _SCRIPT]


def test_pct_policy_repeats_itself_for_the_same_configuration() -> None:
    # Determinism is the whole premise: a campaign that reports a seed must
    # be able to hand that seed back and get the identical schedule.
    first = _drive(PCTPolicy(seed=41, depth=3, horizon=64))
    second = _drive(PCTPolicy(seed=41, depth=3, horizon=64))
    assert first == second
    assert len(set(first)) > 1  # not a constant sequence dressed up as agreement


def test_pct_policy_explores_differently_under_different_seeds() -> None:
    # Priorities come from the seed, so seeds are what make a campaign cover
    # more than one schedule. One differing run is enough to prove it.
    baseline = _drive(PCTPolicy(seed=0, depth=3, horizon=64))
    assert any(
        _drive(PCTPolicy(seed=seed, depth=3, horizon=64)) != baseline
        for seed in range(1, 12)
    )


def test_pct_policy_keeps_running_the_same_owner_when_nothing_demotes_it() -> None:
    # The property that separates PCT from a uniform draw: priorities are
    # drawn once and held, so with no change points one owner runs and runs.
    # depth=1 means zero demotions, which makes "held" checkable exactly.
    policy = PCTPolicy(seed=7, depth=1, horizon=16)
    queue = _owned(11, 22)
    winners = [queue[policy.choose(queue)][0] for _ in range(12)]
    assert winners == [winners[0]] * 12
    assert winners[0] in (11, 22)


def test_pct_policy_demotes_the_leader_at_a_change_point() -> None:
    # depth=2, horizon=4 puts the single change point inside the first four
    # steps, so a twelve-step run must contain the handover.
    policy = PCTPolicy(seed=3, depth=2, horizon=4)
    queue = _owned(11, 22)
    winners = [queue[policy.choose(queue)][0] for _ in range(12)]
    assert len(set(winners)) == 2, winners
    handover = next(step for step, w in enumerate(winners) if w != winners[0])
    # Demotion is permanent: the old leader sits below every fresh priority
    # and below every later demotion, so it never leads again.
    assert winners[handover:] == [winners[handover]] * (12 - handover)


def test_pct_policy_lowers_each_demotion_below_the_last() -> None:
    # depth=3, horizon=3 puts two distinct change points inside the first
    # three steps, so a six-step run actually reaches both and builds a
    # ladder rather than a single step down.
    policy = PCTPolicy(seed=2, depth=3, horizon=3)
    queue = _owned(11, 22, 33)
    winners = [queue[policy.choose(queue)][0] for _ in range(6)]
    # A demoted owner cannot lead again, so each change point hands the
    # queue to someone new: three owners, three reigns, in demotion order.
    leaders = list(dict.fromkeys(winners))
    assert len(leaders) == 3, winners
    first_demoted, second_demoted, _ = leaders
    # The owner pushed off first must still outrank the one pushed off
    # second — that is what the descending floor buys. Asked in the order
    # that makes the answer mean something: a policy that dropped both to
    # one shared floor would tie, and a tie takes index 0.
    probe = _owned(second_demoted, first_demoted)
    assert probe[policy.choose(probe)][0] == first_demoted


def test_pct_policy_gives_a_newcomer_a_priority_that_competes() -> None:
    # Tasks are created mid-run, so an owner seen for the first time has to
    # be dealt in — with a priority drawn from the same range as everyone
    # else's, which means it sometimes wins immediately and sometimes loses.
    established = _owned(11, 22)
    joined = _owned(11, 22, 33)
    newcomer_wins = 0
    for seed in range(24):
        policy = PCTPolicy(seed=seed, depth=1, horizon=16)
        for _ in range(5):
            policy.choose(established)
        leader = established[policy.choose(established)][0]
        winner = joined[policy.choose(joined)][0]
        # Nothing about the newcomer may reorder the owners already ranked.
        assert winner in (leader, 33)
        newcomer_wins += winner == 33
    assert 0 < newcomer_wins < 24


def test_pct_policy_draws_a_fresh_priority_for_every_unowned_callback() -> None:
    # Unowned entries carry an id unique to the callback — the loop hands
    # out a fresh one per queued callback, never a repeat — so under PCT
    # each is its own one-step chain: a fresh draw, a fresh chance to run
    # first. A queue of them therefore reshuffles instead of settling.
    policy = PCTPolicy(seed=5, depth=1, horizon=16)
    seq = iter(range(1000))
    choices = [
        policy.choose([(-1 - next(seq), "callback") for _ in range(4)])
        for _ in range(16)
    ]
    assert len(set(choices)) > 1


def test_pct_policy_is_a_scheduling_policy() -> None:
    # The loop only ever holds a policy through the protocol, so the binding
    # below is the assertion: it is mypy that has to accept it.
    policy: SchedulingPolicy = PCTPolicy(seed=0)
    assert policy.choose(_owned(4, 9)) in (0, 1)
    assert policy.diverged_at is None


def test_pct_policy_never_diverges() -> None:
    # Divergence describes a replay that stopped matching its recording;
    # a policy that generates its own schedule has nothing to depart from.
    policy = PCTPolicy(seed=1, depth=3, horizon=64)
    _drive(policy)
    assert policy.diverged_at is None


def test_pct_policy_rejects_a_degenerate_configuration() -> None:
    with pytest.raises(ValueError, match="depth"):
        PCTPolicy(seed=1, depth=0)
    with pytest.raises(ValueError, match="depth"):
        PCTPolicy(seed=1, depth=-2)
    with pytest.raises(ValueError, match="horizon"):
        PCTPolicy(seed=1, horizon=0)
    with pytest.raises(ValueError, match="horizon"):
        PCTPolicy(seed=1, horizon=-5)
    # Change points are distinct, so a horizon with fewer steps than the
    # depth needs cannot deliver the depth that was asked for. Saying so
    # beats silently scheduling at a shallower depth than the caller thinks.
    with pytest.raises(ValueError, match="change points"):
        PCTPolicy(seed=1, depth=5, horizon=3)
    PCTPolicy(seed=1, depth=4, horizon=3)  # exactly enough room is enough


# ----------------------------------------------------------------------
# What the loop shows the policy
# ----------------------------------------------------------------------


class _Recording:
    """Keeps every ready view the loop offers, then takes the oldest.

    FIFO rather than a draw, because what is under test is the view the loop
    builds and not the order a policy picks from it.
    """

    def __init__(self) -> None:
        self.diverged_at: int | None = None
        self.steps: list[tuple[ReadyView, ...]] = []

    def choose(self, ready: Sequence[ReadyView]) -> int:
        self.steps.append(tuple(ready))
        return 0


# One step's worth of probing: the count, the whole thing iterated, the first
# entry, the first entry again, the last entry, and a slice of all of it.
_Read = tuple[int, list[ReadyView], ReadyView, ReadyView, ReadyView, list[ReadyView]]


class _Probing:
    """Reads the ready sequence every way a policy might, and keeps what it saw."""

    def __init__(self) -> None:
        self.diverged_at: int | None = None
        self.reads: list[_Read] = []

    def choose(self, ready: Sequence[ReadyView]) -> int:
        self.reads.append(
            (
                len(ready),
                list(ready),  # iteration
                ready[0],
                ready[0],  # the same entry a second time
                ready[-1],  # counted from the end
                list(ready[:]),  # a slice
            )
        )
        return 0


async def _two_workers_and_two_callbacks() -> list[str]:
    """Two tasks taking several steps each, alongside two bare callbacks."""
    loop = asyncio.get_running_loop()
    log: list[str] = []
    loop.call_soon(log.append, "bare:first")
    loop.call_soon(log.append, "bare:second")

    async def worker(name: str) -> None:
        for number in range(3):
            await asyncio.sleep(0)
            log.append(f"{name}:{number}")

    first = asyncio.create_task(worker("one"), name="one")
    second = asyncio.create_task(worker("two"), name="two")
    await first
    await second
    return log


def _run_recorded() -> tuple[SimLoop, _Recording]:
    loop = SimLoop(seed=3)
    policy = _Recording()
    loop._policy = policy
    try:
        loop.run_until_complete(_two_workers_and_two_callbacks())
    finally:
        loop.close()
    return loop, policy


def test_the_views_line_up_with_the_ready_queue() -> None:
    loop, policy = _run_recorded()
    ran = [event.label for event in loop.trace if event.kind == "run"]
    assert not any(event.kind == "cancel" for event in loop.trace)
    # The index a policy returns indexes the views it was handed, so the view
    # at that index must describe the callback the step actually ran. This
    # policy always takes the first, so the first view of every step names
    # what ran — position for position, in step order.
    assert [step[0][1] for step in policy.steps] == ran
    assert all(len(step) > 0 for step in policy.steps)


def test_the_ready_sequence_reads_the_way_a_sequence_should() -> None:
    # Views are built only where a policy looks, so every way of looking has
    # to agree — and looking twice has to give the same answer.
    loop = SimLoop(seed=3)
    policy = _Probing()
    loop._policy = policy
    try:
        loop.run_until_complete(_two_workers_and_two_callbacks())
    finally:
        loop.close()
    assert any(count > 1 for count, *_ in policy.reads)  # not all queues of one
    for count, iterated, first, again, last, sliced in policy.reads:
        assert len(iterated) == count
        assert first == iterated[0]
        assert again == first
        assert last == iterated[-1]
        assert sliced == iterated


def test_the_ready_sequence_follows_the_live_queue() -> None:
    loop = SimLoop(seed=3)
    views = loop._ready_views
    assert len(views) == 0
    # One sequence serves every step, reading the queue in place rather than
    # copying it, which is what makes a step cost no allocation.
    handle = loop.call_soon(print)
    assert len(views) == 1
    assert views[0][1] == "print"
    with pytest.raises(IndexError):
        views[1]  # what stops the Sequence mixin's iteration
    handle.cancel()
    loop.close()


def test_every_step_of_a_task_carries_that_task_s_id() -> None:
    _, policy = _run_recorded()
    owners = {owner for step in policy.steps for owner, _ in step if owner >= 0}
    # The task run_until_complete wrapped the coroutine in, plus the two
    # workers. An id that changed between a task's own steps would show up
    # here as a fourth, and one shared between tasks as a second.
    assert owners == {0, 1, 2}
    assert sum(1 for step in policy.steps for owner, _ in step if owner >= 0) > 10


def test_a_callback_no_task_owns_gets_a_negative_id_of_its_own() -> None:
    _, policy = _run_recorded()
    bare = {
        (owner, label)
        for step in policy.steps
        for owner, label in step
        if label == "list.append"
    }
    assert len(bare) == 2  # one per call_soon, each with an id of its own
    assert all(owner < 0 for owner, _ in bare)


def test_the_loop_numbers_tasks_in_creation_order() -> None:
    loop = SimLoop(seed=3)
    tasks: list[asyncio.Task[None]] = []

    async def main() -> None:
        async def noop() -> None:
            pass

        tasks.extend(asyncio.create_task(noop()) for _ in range(3))
        for task in tasks:
            await task

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    # 0 went to the task run_until_complete wrapped the coroutine in.
    assert [loop._task_owner(task) for task in tasks] == [1, 2, 3]


def test_the_owner_map_does_not_keep_a_finished_task_alive() -> None:
    loop = SimLoop(seed=3)
    watched: list[weakref.ref[asyncio.Task[None]]] = []

    async def main() -> None:
        async def noop() -> None:
            pass

        for _ in range(4):
            task: asyncio.Task[None] = asyncio.create_task(noop())
            watched.append(weakref.ref(task))
            await task
        await asyncio.sleep(0)  # let the last task's done callbacks run

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    gc.collect()
    # A campaign runs millions of tasks through one loop at a time, so the
    # map that names them must never be the thing keeping one alive.
    assert [reference() for reference in watched] == [None] * 4
    assert len(loop._task_owners) == 1  # the wrapper task run_until_complete holds


# ----------------------------------------------------------------------
# Loop integration
# ----------------------------------------------------------------------

_WORKERS = ("alice", "bob", "carol")
_ROUNDS = 3


async def _mixed() -> list[str]:
    """A workload that keeps several callbacks ready at once.

    Bare callbacks, timers landing on shared deadlines and queue-blocked
    tasks all compete, so the scheduler makes a decision on nearly every
    step instead of running a single obvious candidate.
    """
    loop = asyncio.get_running_loop()
    log: list[str] = []
    inbox: asyncio.Queue[str] = asyncio.Queue()

    async def worker(name: str) -> None:
        for number in range(_ROUNDS):
            loop.call_soon(log.append, f"{name}:soon:{number}")
            loop.call_later(0.01 * (number + 1), log.append, f"{name}:timer:{number}")
            await inbox.put(f"{name}:{number}")
            await asyncio.sleep(0.005)

    async def collect(total: int) -> None:
        for _ in range(total):
            log.append(f"got:{await inbox.get()}")

    collector = asyncio.create_task(collect(len(_WORKERS) * _ROUNDS))
    for task in [asyncio.create_task(worker(name)) for name in _WORKERS]:
        await task
    await collector
    await asyncio.sleep(0.05)  # let the trailing timers fire
    return log


def _run_mixed(loop: SimLoop) -> tuple[str, list[str]]:
    try:
        log = loop.run_until_complete(_mixed())
    finally:
        loop.close()
    return loop.trace_hash(), log


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[str] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append(data.decode())


async def _bind(
    host: Host, port: int, factory: Callable[[], asyncio.DatagramProtocol]
) -> asyncio.DatagramTransport:
    async def bind() -> asyncio.DatagramTransport:
        endpoint: tuple[asyncio.DatagramTransport, asyncio.DatagramProtocol] = (
            await asyncio.get_running_loop().create_datagram_endpoint(
                factory, local_addr=("0.0.0.0", port)
            )
        )
        return endpoint[0]

    task: asyncio.Task[asyncio.DatagramTransport] = host.create_task(bind())
    return await task


def _run_net(loop: SimLoop) -> tuple[str, list[str], list[str]]:
    """Send datagrams across a lossy, jittery link and report what arrived."""
    alpha = loop.net.host("alpha")
    beta = loop.net.host("beta")
    loop.net.set_defaults(latency=(0.001, 0.05))
    loop.net.set_link("beta", "alpha", drop=0.25, duplicate=0.25)
    collector = _Collector()

    async def main() -> list[str]:
        transport_a = await _bind(alpha, 7000, lambda: collector)
        transport_b = await _bind(beta, 7001, asyncio.DatagramProtocol)

        async def send_all() -> None:
            for number in range(12):
                transport_b.sendto(f"ping{number:02d}".encode(), ("alpha", 7000))
                await asyncio.sleep(0.01)

        await beta.create_task(send_all())
        await asyncio.sleep(1.0)
        transport_a.close()
        transport_b.close()
        await asyncio.sleep(0.01)
        return collector.received

    try:
        received = loop.run_until_complete(main())
    finally:
        loop.close()
    faults = [event.label for event in loop.trace if event.kind == "net"]
    return loop.trace_hash(), received, faults


def test_a_seeded_loop_records_its_choices() -> None:
    loop = SimLoop(seed=41)
    _run_mixed(loop)
    choices = loop._choices
    assert len(choices) > 50
    assert all(choice >= 0 for choice in choices)
    assert loop._diverged_at is None


def test_recorded_choices_replay_to_an_identical_trace() -> None:
    original = SimLoop(seed=41)
    original_hash, original_log = _run_mixed(original)

    replay = SimLoop._from_choices(original._choices, 41)
    replay_hash, replay_log = _run_mixed(replay)

    assert replay_hash == original_hash
    assert replay_log == original_log
    assert replay._choices == original._choices
    assert replay._diverged_at is None
    assert replay.seed == 41


def test_replayed_loop_keeps_the_seeded_fault_stream() -> None:
    original = SimLoop(seed=7)
    original_hash, original_received, faults = _run_net(original)
    assert any(label.startswith("drop") for label in faults)
    assert any(label.startswith("dup") for label in faults)

    replay = SimLoop._from_choices(original._choices, 7)
    replay_hash, replay_received, replay_faults = _run_net(replay)

    assert replay_hash == original_hash
    assert replay_received == original_received
    assert replay_faults == faults
    assert replay._diverged_at is None


def test_an_out_of_range_choice_completes_the_run_and_marks_divergence() -> None:
    original = SimLoop(seed=41)
    _, original_log = _run_mixed(original)
    choices = list(original._choices)
    perturbed = len(choices) // 2
    choices[perturbed] = MAX_CHOICE  # past any ready queue this workload builds

    replay = SimLoop._from_choices(choices, 41)
    _, replay_log = _run_mixed(replay)

    assert replay._diverged_at == perturbed
    assert sorted(replay_log) == sorted(original_log)  # every callback still ran


def test_a_truncated_recording_finishes_under_fifo() -> None:
    original = SimLoop(seed=41)
    _, original_log = _run_mixed(original)
    kept = len(original._choices) // 2

    replay = SimLoop._from_choices(original._choices[:kept], 41)
    _, replay_log = _run_mixed(replay)

    assert replay._diverged_at == kept
    assert sorted(replay_log) == sorted(original_log)
