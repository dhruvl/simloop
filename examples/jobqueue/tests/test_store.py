"""The fenced, idempotent effect sink and its ablatable checks."""

from __future__ import annotations

from jobqueue.store import EffectStore


def test_first_commit_is_accepted() -> None:
    store = EffectStore()
    assert store.begin("j1", 1) is True
    assert store.begun == [("j1", 1)]
    assert store.commit("j1", 1, "v") == "ok"
    assert len(store.commits) == 1
    assert store.commits[0].job_id == "j1"
    assert store.commits[0].stale is False


def test_second_commit_for_a_job_is_duplicate() -> None:
    store = EffectStore()
    store.begin("j1", 1)
    store.commit("j1", 1, "v")
    store.begin("j1", 2)
    assert store.commit("j1", 2, "v") == "duplicate"
    assert len(store.commits) == 1
    assert ("j1", 2, "duplicate") in store.rejected


def test_stale_commit_is_fenced_out() -> None:
    store = EffectStore()
    store.begin("j1", 1)
    store.begin("j1", 2)
    assert store.commit("j1", 1, "v") == "stale"
    assert store.commits == []
    assert ("j1", 1, "stale") in store.rejected


def test_stale_begin_is_fenced_out() -> None:
    store = EffectStore()
    store.begin("j1", 2)
    assert store.begin("j1", 1) is False
    assert ("j1", 1, "stale-begin") in store.rejected
    assert store.begun == [("j1", 2)]


def test_unfenced_store_accepts_but_flags_zombie_writes() -> None:
    store = EffectStore(fenced=False)
    store.begin("j1", 1)
    store.begin("j1", 2)
    assert store.commit("j1", 1, "v") == "ok"
    assert store.commits[0].stale is True


def test_unidempotent_store_double_commits() -> None:
    store = EffectStore(idempotent=False)
    store.begin("j1", 1)
    store.commit("j1", 1, "v")
    store.begin("j1", 2)
    assert store.commit("j1", 2, "v") == "ok"
    assert len(store.commits) == 2
    assert store.commits[1].stale is False
