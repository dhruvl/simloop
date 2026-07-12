import asyncio
import time
import uuid as uuid_module

from simloop import SimLoop, sim


def _collect(seed: int) -> tuple[list[float], list[str], float]:
    async def main() -> tuple[list[float], list[str], float]:
        draws = [sim.random.random() for _ in range(5)]
        uuids = [str(sim.uuid4()) for _ in range(3)]
        await asyncio.sleep(1.5)
        return draws, uuids, sim.time()

    loop = SimLoop(seed)
    try:
        result: tuple[list[float], list[str], float] = loop.run_until_complete(main())
        return result
    finally:
        loop.close()


def test_shims_replay_exactly_per_seed() -> None:
    assert _collect(1) == _collect(1)


def test_shims_diverge_across_seeds() -> None:
    assert _collect(1) != _collect(2)


def test_sim_time_is_virtual_inside_a_run() -> None:
    assert _collect(0)[2] == 1.5


def test_sim_uuid4_is_a_valid_version_4_uuid() -> None:
    for text in _collect(3)[1]:
        assert uuid_module.UUID(text).version == 4


def test_user_draws_do_not_perturb_scheduling() -> None:
    async def with_draws() -> None:
        for _ in range(10):
            sim.random.random()
            sim.uuid4()
        await asyncio.sleep(1.0)

    async def without_draws() -> None:
        await asyncio.sleep(1.0)

    hashes = []
    for main in (with_draws, without_draws):
        loop = SimLoop(seed=0)
        try:
            loop.run_until_complete(main())
        finally:
            loop.close()
        hashes.append(loop.trace_hash())
    assert hashes[0] == hashes[1]


def test_shims_fall_back_to_stdlib_outside_a_simulation() -> None:
    assert len({sim.random.random() for _ in range(3)}) == 3
    assert sim.uuid4() != sim.uuid4()
    assert abs(sim.time() - time.time()) < 5.0


def test_shims_fall_back_on_the_stock_loop() -> None:
    async def main() -> float:
        sim.random.random()
        assert sim.uuid4() != sim.uuid4()
        return sim.time()

    assert abs(asyncio.run(main()) - time.time()) < 5.0
