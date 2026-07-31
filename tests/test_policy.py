import asyncio
import random
from collections.abc import Callable

import pytest

from simloop import Host, SimLoop
from simloop._policy import MAX_CHOICE, ScriptedPolicy, SeededPolicy


def test_seeded_policy_matches_a_bare_random_stream() -> None:
    # The seam only holds if the policy draws exactly what the loop used to
    # draw: same seed, same calls, same values, in the same order.
    sizes = [1, 2, 3, 7, 2, 64, 5, 1, 12, 3, 9]
    reference = random.Random(41)
    policy = SeededPolicy(41)
    assert [policy.choose(n) for n in sizes] == [
        reference.randrange(n) for n in sizes
    ]


def test_seeded_policy_never_diverges() -> None:
    policy = SeededPolicy(0)
    for _ in range(10):
        policy.choose(4)
    assert policy.diverged_at is None


def test_scripted_policy_replays_its_recording() -> None:
    policy = ScriptedPolicy([2, 0, 1])
    assert [policy.choose(3), policy.choose(4), policy.choose(2)] == [2, 0, 1]
    assert policy.diverged_at is None


def test_scripted_policy_clamps_choices_past_the_ready_queue() -> None:
    policy = ScriptedPolicy([5, 1])
    assert policy.choose(3) == 2
    assert policy.diverged_at == 0
    assert policy.choose(2) == 1


def test_scripted_policy_marks_only_the_first_divergence() -> None:
    policy = ScriptedPolicy([0, 9, 9])
    assert [policy.choose(4), policy.choose(2), policy.choose(2)] == [0, 1, 1]
    assert policy.diverged_at == 1


def test_exhausted_scripted_policy_falls_back_to_fifo() -> None:
    policy = ScriptedPolicy([1])
    assert policy.choose(3) == 1
    assert policy.diverged_at is None
    assert [policy.choose(3), policy.choose(3)] == [0, 0]
    assert policy.diverged_at == 1


def test_empty_recording_is_pure_fifo() -> None:
    policy = ScriptedPolicy([])
    assert policy.choose(5) == 0
    assert policy.diverged_at == 0


def test_scripted_policy_rejects_impossible_choices() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        ScriptedPolicy([0, -1])
    with pytest.raises(ValueError, match="too large"):
        ScriptedPolicy([MAX_CHOICE + 1])


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
