"""Versions as a Redis flag: sticky cohorts, the soak timer, and rollback as one write."""
import time

import rollout
from flag_cache import CachedVersions

USERS = [f"user-{i}" for i in range(2000)]


def on_candidate(r) -> set[str]:
    return {u for u in USERS if rollout.version_for(r, u) == "model-2"}


def test_cohorts_are_sticky_and_widening_only_adds_users(r):
    rollout.start(r, stable="model-1", candidate="model-2", percent=5, now=1000.0)
    at_5 = on_candidate(r)
    assert at_5 == on_candidate(r)                           # same answer on every request
    assert 0.03 < len(at_5) / len(USERS) < 0.07
    assert rollout.widen(r, 25, soak_seconds=600, now=1700.0)
    at_25 = on_candidate(r)
    assert at_5 < at_25                                      # nobody flips back to stable
    assert 0.21 < len(at_25) / len(USERS) < 0.29


def test_widen_waits_for_the_soak_period(r):
    rollout.start(r, "model-1", "model-2", percent=1, now=1000.0)
    assert not rollout.widen(r, 5, soak_seconds=3600, now=1000.0 + 3599)
    assert r.hget(rollout.KEY, "percent") == b"1"
    assert rollout.widen(r, 5, soak_seconds=3600, now=1000.0 + 3600)
    assert not rollout.widen(r, 25, soak_seconds=3600, now=1000.0 + 3700)   # the timer restarted
    assert r.hget(rollout.KEY, "percent") == b"5"


def test_rollback_reaches_the_next_request(r):
    rollout.start(r, "model-1", "model-2", percent=100)
    assert rollout.version_for(r, "user-7") == "model-2"
    rollout.rollback(r)
    assert rollout.version_for(r, "user-7") == "model-1"
    assert on_candidate(r) == set()


def test_cached_flag_lags_a_rollback_by_up_to_its_ttl(r):
    rollout.start(r, "model-1", "model-2", percent=100)
    cached = CachedVersions(r, ttl=0.3)
    assert cached.version_for("user-7") == "model-2"
    rollout.rollback(r)
    assert rollout.version_for(r, "user-7") == "model-1"     # a fresh read sees it at once
    assert cached.version_for("user-7") == "model-2"         # the cache doesn't, yet
    time.sleep(0.35)
    assert cached.version_for("user-7") == "model-1"


def test_rollback_is_one_write_and_stops_later_widening(r):
    rollout.start(r, "model-1", "model-2", percent=50, now=1000.0)
    before = r.info("commandstats")["cmdstat_hset"]["calls"]
    rollout.rollback(r)
    assert r.info("commandstats")["cmdstat_hset"]["calls"] == before + 1
    assert not rollout.widen(r, 100, soak_seconds=0, now=9999.0)     # halted: no way back up
    assert on_candidate(r) == set()
    rollout.start(r, "model-1", "model-3", percent=1, now=10_000.0)  # a new rollout starts clean
    assert rollout.widen(r, 5, soak_seconds=60, now=10_060.0)
