"""Replay-stability checks for the simulated network (slow ones run in CI)."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).with_name("replay_net_workload.py")


def _load_workload() -> Any:
    spec = importlib.util.spec_from_file_location("replay_net_workload", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_child(seed: int, hashseed: str | None) -> str:
    env = os.environ.copy()
    env.pop("PYTHONHASHSEED", None)
    if hashseed is not None:
        env["PYTHONHASHSEED"] = hashseed
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), str(seed)],
        capture_output=True,
        text=True,
        env=env,
        check=True,
        timeout=60,
    )
    assert result.stderr == ""
    return result.stdout.strip()


def test_same_seed_replays_identically() -> None:
    workload = _load_workload()
    for seed in range(3):
        assert workload.run(seed) == workload.run(seed)


def test_different_seeds_diverge() -> None:
    workload = _load_workload()
    assert len({workload.run(seed) for seed in range(3)}) == 3


@pytest.mark.slow
def test_hundred_network_reruns_per_seed_are_stable() -> None:
    workload = _load_workload()
    for seed in range(5):
        results = {workload.run(seed) for _ in range(100)}
        assert len(results) == 1, f"seed {seed} produced diverging runs"


@pytest.mark.slow
def test_network_replay_is_stable_across_processes_and_hash_seeds() -> None:
    workload = _load_workload()
    for seed in (0, 7):
        results = {_run_child(seed, hs) for hs in (None, "0", "1", "random")}
        results.add(workload.run(seed))
        assert len(results) == 1, f"seed {seed} diverged across processes"
