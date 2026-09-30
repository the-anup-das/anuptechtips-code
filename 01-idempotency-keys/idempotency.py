"""Idempotency keys on PostgreSQL with psycopg 3: claim the key atomically, run the
work once, store the response beside the side effect, and replay it to every retry."""
import hashlib
import json
from collections.abc import Callable
from datetime import timedelta
from typing import Any, NamedTuple

import psycopg
from psycopg.types.json import Jsonb

LEASE = timedelta(seconds=30)  # longer than your slowest do_work


class Result(NamedTuple):
    code: int
    body: dict[str, Any]
    replayed: bool = False


class LeaseLost(Exception):
    """Our claim went stale and another request took it over, so we rolled back."""


def fingerprint(method: str, path: str, body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{method} {path} {canonical}".encode()).hexdigest()


def replay_or_reject(fp: str, stored_fp: str, status: str,
                     code: int | None, body: dict | None) -> Result:
    """The answer for a key we have seen before."""
    if stored_fp != fp:
        return Result(422, {"error": "Idempotency-Key was used with a different request"})
    if status == "in_progress":
        return Result(409, {"error": "A request with this Idempotency-Key is in progress"})
    return Result(code, body, replayed=True)


# Insert a new claim, or take over a stale one. Anything else returns no row.
CLAIM = """
INSERT INTO idempotency_keys (client_id, idem_key, fingerprint)
VALUES (%(client)s, %(key)s, %(fp)s)
ON CONFLICT (client_id, idem_key) DO UPDATE SET locked_at = now()
    WHERE idempotency_keys.status = 'in_progress'
      AND idempotency_keys.fingerprint = excluded.fingerprint
      AND idempotency_keys.locked_at < now() - %(lease)s
RETURNING locked_at
"""


def run_once(conn: psycopg.Connection, client_id: str, key: str, fp: str,
             do_work: Callable[[psycopg.Connection], tuple[int, dict]],
             lease: timedelta = LEASE) -> Result:
    # The claim must commit on its own, so a duplicate gets its 409 straight away
    # instead of waiting on the unique index until our transaction ends.
    if not conn.autocommit:
        raise ValueError("run_once needs a connection opened with autocommit=True")

    args = {"client": client_id, "key": key, "fp": fp, "lease": lease}
    claimed = conn.execute(CLAIM, args).fetchone()

    if claimed is None:  # someone else holds or finished this key
        row = conn.execute(
            "SELECT fingerprint, status, response_code, response_body "
            "FROM idempotency_keys WHERE client_id = %(client)s AND idem_key = %(key)s",
            args,
        ).fetchone()
        if row is None:  # released a moment ago
            return Result(409, {"error": "Retry this request"})
        return replay_or_reject(fp, *row)

    args["token"] = claimed[0]  # our lease; completion checks that we still hold it
    try:
        with conn.transaction():  # business writes and stored response commit together
            code, body = do_work(conn)
            done = conn.execute(
                "UPDATE idempotency_keys SET status = 'completed', "
                "response_code = %(code)s, response_body = %(body)s "
                "WHERE client_id = %(client)s AND idem_key = %(key)s "
                "AND locked_at = %(token)s",
                {**args, "code": code, "body": Jsonb(body)},
            )
            if done.rowcount != 1:
                raise LeaseLost(key)  # rolls back do_work's writes as well
        return Result(code, body)
    except Exception:
        # Rolled back, so nothing happened: free the key and let a retry run.
        conn.execute(
            "DELETE FROM idempotency_keys WHERE client_id = %(client)s "
            "AND idem_key = %(key)s AND locked_at = %(token)s",
            args,
        )
        raise


def purge_expired(conn: psycopg.Connection, keep: timedelta = timedelta(hours=24)) -> int:
    """Delete keys older than your published retention. Run it from a scheduled job."""
    cur = conn.execute("DELETE FROM idempotency_keys WHERE created_at < now() - %s", (keep,))
    return cur.rowcount
