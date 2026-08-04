"""Replay-stability checks for a TLS workload (slow ones run in CI)."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import _tls_certs

_SCRIPT = Path(__file__).with_name("replay_tls_workload.py")


def _load_workload() -> Any:
    spec = importlib.util.spec_from_file_location("replay_tls_workload", _SCRIPT)
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
        timeout=120,
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


def test_a_fresh_certificate_does_not_move_the_hash() -> None:
    # The trace records how many packets a handshake made and in what order,
    # never their bytes, and the TLS engine emits one packet per flight — so
    # new key material changes every byte on the wire and nothing in the hash.
    workload = _load_workload()
    first = workload.run(0)
    _tls_certs.forget()
    assert workload.run(0) == first


@pytest.mark.slow
def test_hundred_tls_reruns_per_seed_are_stable() -> None:
    workload = _load_workload()
    for seed in range(3):
        results = {workload.run(seed) for _ in range(100)}
        assert len(results) == 1, f"seed {seed} produced diverging runs"


@pytest.mark.slow
def test_tls_replay_is_stable_across_processes_and_hash_seeds() -> None:
    workload = _load_workload()
    for seed in (0, 7):
        results = {_run_child(seed, hs) for hs in (None, "0", "1", "random")}
        results.add(workload.run(seed))
        assert len(results) == 1, f"seed {seed} diverged across processes"
