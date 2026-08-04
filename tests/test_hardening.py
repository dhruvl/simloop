"""Replay-stability checks for the loop-only workload (slow ones run in CI)."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).with_name("replay_workload.py")


def _load_workload() -> Any:
    spec = importlib.util.spec_from_file_location("replay_workload", _SCRIPT)
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


# What this workload recorded before TLS existed. It never asks for TLS, and a
# run that does not ask for a feature has to decide exactly what it decided
# without it — the same draws, the same timers, the same trace events. A
# deliberate change to the trace format updates these and says so in the
# changelog.
_RECORDED = {
    0: (
        "57ce056be9fbc450b6ce34595584c6b64cd64c69f1df3c8c7191cdab7595c36a "
        "f7e191e5a6c84b14734ff477b5da89985d7567cc72c5b8a1725c462cd22625de"
    ),
    7: (
        "bdbac6965559225cfa661dbc8be7448665d05abb6b5afaa44fdd8e56384df067 "
        "784a003bd907cb4818d1a06511be9bc9bd91bd0609d0b286153b484f1a469406"
    ),
}


def test_the_recorded_runs_still_replay() -> None:
    workload = _load_workload()
    for seed, recorded in _RECORDED.items():
        assert workload.run(seed) == recorded, f"seed {seed} no longer replays"


@pytest.mark.slow
def test_hundred_reruns_per_seed_are_stable() -> None:
    workload = _load_workload()
    for seed in range(5):
        results = {workload.run(seed) for _ in range(100)}
        assert len(results) == 1, f"seed {seed} produced diverging runs"


@pytest.mark.slow
def test_replay_is_stable_across_processes_and_hash_seeds() -> None:
    workload = _load_workload()
    for seed in (0, 7):
        results = {_run_child(seed, hs) for hs in (None, "0", "1", "random")}
        results.add(workload.run(seed))
        assert len(results) == 1, f"seed {seed} diverged across processes"
