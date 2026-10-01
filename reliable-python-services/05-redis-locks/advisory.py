"""The no-Redis option: a Postgres advisory lock that ends with the transaction.

    with psycopg.connect(PG_DSN) as db, db.transaction():
        if try_lock(db, "nightly-report"):
            build_report(db)   # released at COMMIT or ROLLBACK, or if the connection dies
"""
import hashlib

import psycopg


def lock_id(name: str) -> int:
    # Not hash(name): str hashes are salted per process, so each worker would get a different id.
    return int.from_bytes(hashlib.blake2b(name.encode(), digest_size=8).digest(), "big", signed=True)


def try_lock(db: psycopg.Connection, name: str) -> bool:
    """Call inside a transaction. True: the lock is ours until the transaction ends."""
    return db.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id(name),)).fetchone()[0]
