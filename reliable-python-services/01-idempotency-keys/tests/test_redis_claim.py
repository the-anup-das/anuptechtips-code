"""run_once_redis against Redis 8 in Docker (the charge itself still lands in Postgres)."""
import json
import logging

import psycopg
import pytest
import redis

from app import DSN
from helpers import REDIS_URL, all_at_once, charge_work, charges
from idempotency import fingerprint
from redis_claim import KEEP_SECONDS, LEASE_SECONDS, SWAP_IF_OURS, run_once_redis

FP = fingerprint("POST", "/charges", {"amount": 1000, "currency": "usd"})
RKEY = "idem:c1:k1"


def work(pg, client="c1"):
    return lambda: charge_work(client)(pg)


def test_new_key_runs_once_and_keeps_the_result_for_24_hours(r, pg):
    result = run_once_redis(r, "c1", "k1", FP, work(pg))
    assert result.code == 201 and not result.replayed
    rec = json.loads(r.get(RKEY))
    assert rec["status"] == "completed" and rec["body"] == result.body
    assert KEEP_SECONDS - 5 < r.ttl(RKEY) <= KEEP_SECONDS
    assert charges(pg, "c1") == 1


def test_retry_is_replayed(r, pg):
    first = run_once_redis(r, "c1", "k1", FP, work(pg))
    again = run_once_redis(r, "c1", "k1", FP, work(pg))
    assert again.replayed and again.body == first.body
    assert charges(pg, "c1") == 1


def test_different_payload_is_422(r, pg):
    run_once_redis(r, "c1", "k1", FP, work(pg))
    assert run_once_redis(r, "c1", "k1", "other-fp", work(pg)).code == 422
    assert charges(pg, "c1") == 1


def test_in_progress_claim_is_409_and_expires_after_the_lease(r, pg):
    seen = {}

    def work_that_peeks():
        seen["ttl"] = r.ttl(RKEY)
        seen["dup"] = run_once_redis(r, "c1", "k1", FP, work(pg))
        return work(pg)()

    run_once_redis(r, "c1", "k1", FP, work_that_peeks)
    assert seen["dup"].code == 409
    assert LEASE_SECONDS - 5 < seen["ttl"] <= LEASE_SECONDS


def test_exception_releases_the_key(r, pg):
    def boom():
        raise RuntimeError("card network down")

    with pytest.raises(RuntimeError):
        run_once_redis(r, "c1", "k1", FP, boom)
    assert not r.exists(RKEY)
    assert run_once_redis(r, "c1", "k1", FP, work(pg)).code == 201


def test_a_result_never_overwrites_someone_elses_claim(r, pg, caplog):
    def slow_work():
        # Our lease ran out and another request claimed the key.
        r.set(RKEY, json.dumps({"status": "in_progress", "fp": FP, "owner": "someone-else"}))
        return work(pg)()

    with caplog.at_level(logging.WARNING):
        result = run_once_redis(r, "c1", "k1", FP, slow_work)
    assert result.code == 201  # our charge did happen, so report it
    assert json.loads(r.get(RKEY))["owner"] == "someone-else"
    assert "expired mid-work" in caplog.text


def test_concurrent_same_key_requests_run_once(r, pg):
    def attempt(i, barrier):
        client = redis.Redis.from_url(REDIS_URL)
        with psycopg.connect(DSN, autocommit=True) as conn:
            barrier.wait()
            return run_once_redis(client, "c1", "k1", FP, lambda: charge_work("c1")(conn))

    results = all_at_once(20, attempt)
    assert charges(pg, "c1") == 1
    assert sum(1 for res in results if res.code == 201 and not res.replayed) == 1
    assert {res.code for res in results} <= {201, 409}


def test_redis_84_set_ifeq_does_the_same_compare_and_set(r):
    """On Redis 8.4+, SET ... IFEQ replaces the Lua script's compare-and-set."""
    r.set(RKEY, "our-claim")
    assert r.set(RKEY, "done", ifeq="not-ours", ex=60) is None
    assert r.set(RKEY, "done", ifeq="our-claim", ex=60) is True
    assert r.get(RKEY) == b"done"
    swap = r.register_script(SWAP_IF_OURS)  # and the Lua version agrees
    assert swap(keys=[RKEY], args=["not-ours", "x", 60]) == 0
