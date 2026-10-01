"""The claim under concurrency: check-then-insert charges more than once, the atomic
claims charge exactly once. (measure_race.py repeats this over HTTP, 20 runs each.)"""
import psycopg
import redis

from app import DSN
from helpers import REDIS_URL, all_at_once, charge_work, charges
from idempotency import fingerprint, run_once
from naive import check_then_insert
from redis_claim import run_once_redis

FP = fingerprint("POST", "/charges", {"amount": 1000, "currency": "usd"})
N = 20


def race(strategy):
    def attempt(i, barrier):
        with psycopg.connect(DSN, autocommit=True) as conn:
            barrier.wait()
            if strategy == "redis":
                client = redis.Redis.from_url(REDIS_URL)
                return run_once_redis(client, "race", "k1", FP, lambda: charge_work("race")(conn))
            fn = check_then_insert if strategy == "naive" else run_once
            return fn(conn, "race", "k1", FP, charge_work("race"))

    return all_at_once(N, attempt)


def test_check_then_insert_charges_more_than_once(pg):
    results = race("naive")
    duplicates = [res for res in results if isinstance(res, psycopg.errors.UniqueViolation)]
    assert charges(pg, "race") > 1
    # The primary key did fire, but only after those requests had charged the card.
    assert len(duplicates) == charges(pg, "race") - 1


def test_insert_on_conflict_charges_once(pg):
    results = race("postgres")
    assert charges(pg, "race") == 1
    assert sum(1 for res in results if res.code == 201 and not res.replayed) == 1


def test_redis_set_nx_charges_once(pg, r):
    results = race("redis")
    assert charges(pg, "race") == 1
    assert sum(1 for res in results if res.code == 201 and not res.replayed) == 1
