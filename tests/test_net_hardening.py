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


# Captured before write-side flow control existed. The reference workload
# never arms it, and a run that does not ask for it has to decide exactly what
# it decided before — including every fault draw, so a recorded seed a user
# already has still replays.
_RECORDED = {
    0: (
        "d9b64b9f0908ec4ccd605340c35bfe0a140fbdb1c4e5a70cf4e0bf2631b7fd4d "
        "1354346b843e86108cfbf6729db26e95458f1fbae99964c85952f04ed4e44bf8"
    ),
    1: (
        "e5c891d7618f7a15cb571c6dc567f2b2505dfdfb70e8bed9dc99978a28827f1b "
        "67f19e5ed9bf9cc797540e040d43b0c9c92848deb2ba226dc7866a7e4052da4e"
    ),
    2: (
        "6784369a11bfa7dc6f998ff3b606a0b56b0d1d3d55005acabdea44c6e7980b9e "
        "6a9362b769406c2ddc58bb5dcadac8ddf0b5e86da72fee78edf9e15f67adb748"
    ),
}


def test_flow_control_off_reproduces_the_recorded_runs() -> None:
    workload = _load_workload()
    for seed, recorded in _RECORDED.items():
        assert workload.run(seed) == recorded, f"seed {seed} no longer replays"


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
