"""The version that races: look for the key, do the work, then record the key.
Kept here so the race test can show what it costs. Don't ship it."""
from collections.abc import Callable

import psycopg
from psycopg.types.json import Jsonb

from idempotency import Result, replay_or_reject


def check_then_insert(conn: psycopg.Connection, client_id: str, key: str, fp: str,
                      do_work: Callable[[psycopg.Connection], tuple[int, dict]]) -> Result:
    row = conn.execute(
        "SELECT fingerprint, status, response_code, response_body "
        "FROM idempotency_keys WHERE client_id = %s AND idem_key = %s",
        (client_id, key),
    ).fetchone()
    if row is not None:
        return replay_or_reject(fp, *row)

    code, body = do_work(conn)  # every request that got past the SELECT charges the card
    conn.execute(  # the primary key rejects the late ones here, after the charge
        "INSERT INTO idempotency_keys "
        "(client_id, idem_key, fingerprint, status, response_code, response_body) "
        "VALUES (%s, %s, %s, 'completed', %s, %s)",
        (client_id, key, fp, code, Jsonb(body)),
    )
    return Result(code, body)
