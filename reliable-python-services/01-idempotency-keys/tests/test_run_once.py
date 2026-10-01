"""run_once on Postgres: every path the post describes, one test each."""
from datetime import timedelta

import psycopg
import pytest

from app import DSN
from helpers import all_at_once, charge_work, charges
from idempotency import LeaseLost, fingerprint, purge_expired, run_once

FP = fingerprint("POST", "/charges", {"amount": 1000, "currency": "usd"})


def record(pg, client="c1", key="k1"):
    return pg.execute(
        "SELECT status, response_code, response_body FROM idempotency_keys "
        "WHERE client_id = %s AND idem_key = %s", (client, key)).fetchone()


def test_new_key_runs_the_work_and_stores_the_response(pg):
    result = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert result.code == 201 and not result.replayed
    assert charges(pg, "c1") == 1
    assert record(pg) == ("completed", 201, result.body)


def test_retry_replays_the_stored_response_and_runs_nothing(pg):
    first = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    again = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert again.replayed and (again.code, again.body) == (first.code, first.body)
    assert charges(pg, "c1") == 1


def test_same_key_with_a_different_payload_is_422(pg):
    run_once(pg, "c1", "k1", FP, charge_work("c1"))
    other = fingerprint("POST", "/charges", {"amount": 9999, "currency": "usd"})
    result = run_once(pg, "c1", "k1", other, charge_work("c1", 9999))
    assert result.code == 422
    assert charges(pg, "c1") == 1


def test_key_still_in_progress_is_409(pg):
    pg.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint) "
               "VALUES ('c1', 'k1', %s)", (FP,))
    result = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert result.code == 409
    assert charges(pg, "c1") == 0


def test_exception_rolls_back_the_work_and_frees_the_key(pg):
    def fails_after_writing(conn):
        charge_work("c1")(conn)
        raise RuntimeError("card network down")

    with pytest.raises(RuntimeError):
        run_once(pg, "c1", "k1", FP, fails_after_writing)
    assert charges(pg, "c1") == 0 and record(pg) is None

    retry = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert retry.code == 201 and not retry.replayed
    assert charges(pg, "c1") == 1


def test_a_returned_business_error_is_stored_and_replayed(pg):
    declined = lambda conn: (402, {"error": "card_declined"})
    assert run_once(pg, "c1", "k1", FP, declined).code == 402
    again = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert again.replayed and again.code == 402
    assert charges(pg, "c1") == 0


def test_a_stale_claim_is_taken_over(pg):
    # A worker claimed the key an hour ago and crashed before finishing.
    pg.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint, locked_at) "
               "VALUES ('c1', 'k1', %s, now() - interval '1 hour')", (FP,))
    result = run_once(pg, "c1", "k1", FP, charge_work("c1"))
    assert result.code == 201 and not result.replayed
    assert record(pg)[0] == "completed" and charges(pg, "c1") == 1


def test_a_stale_claim_with_a_different_payload_is_still_422(pg):
    pg.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint, locked_at) "
               "VALUES ('c1', 'k1', 'other', now() - interval '1 hour')")
    assert run_once(pg, "c1", "k1", FP, charge_work("c1")).code == 422


def test_a_worker_that_lost_its_lease_rolls_back(pg):
    """A slow worker's lease expires; a retry takes over and finishes. The slow
    worker's charge must roll back instead of landing as a second charge."""
    with psycopg.connect(DSN, autocommit=True) as other:
        def slow_work(conn):
            code, body = charge_work("c1")(conn)
            # While we're still busy, a retry arrives and sees our lease as stale.
            takeover = run_once(other, "c1", "k1", FP, charge_work("c1"), lease=timedelta(0))
            assert takeover.code == 201 and not takeover.replayed
            return code, body

        with pytest.raises(LeaseLost):
            run_once(pg, "c1", "k1", FP, slow_work)
    assert charges(pg, "c1") == 1
    assert record(pg)[0] == "completed"


def test_run_once_refuses_a_connection_without_autocommit():
    with psycopg.connect(DSN) as conn, pytest.raises(ValueError):
        run_once(conn, "c1", "k1", FP, charge_work("c1"))


def test_purge_expired_deletes_only_old_keys(pg):
    pg.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint, created_at) "
               "VALUES ('c1', 'old', 'x', now() - interval '25 hours'), ('c1', 'new', 'x', now())")
    assert purge_expired(pg, keep=timedelta(hours=24)) == 1
    assert [k for (k,) in pg.execute("SELECT idem_key FROM idempotency_keys")] == ["new"]


def test_concurrent_same_key_requests_run_the_work_once(pg):
    def attempt(i, barrier):
        with psycopg.connect(DSN, autocommit=True) as conn:
            barrier.wait()
            return run_once(conn, "c1", "k1", FP, charge_work("c1"))

    results = all_at_once(20, attempt)
    codes = sorted(res.code for res in results)
    assert charges(pg, "c1") == 1
    assert codes.count(201) >= 1 and set(codes) <= {201, 409}
    assert sum(1 for res in results if res.code == 201 and not res.replayed) == 1


def test_an_uncommitted_claim_makes_a_duplicate_insert_wait(pg):
    """Why the claim commits on its own: a duplicate INSERT ... ON CONFLICT waits on
    the unique index until the transaction holding the first insert ends."""
    with psycopg.connect(DSN) as holder, psycopg.connect(DSN, autocommit=True) as dup:
        holder.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint) "
                       "VALUES ('c1', 'k1', 'x')")  # transaction left open
        dup.execute("SET lock_timeout = '300ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            dup.execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint) "
                        "VALUES ('c1', 'k1', 'x') ON CONFLICT DO NOTHING")
        holder.rollback()
