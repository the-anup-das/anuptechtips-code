"""Partition upkeep for the outbox: create days ahead, drop old days safely.

Run both from cron (or use pg_partman). If ensure_partitions stops running,
inserts fail with 'no partition of relation "outbox" found for row', and that
error takes your business transaction down with it.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import psycopg
from psycopg import sql


def partition_name(day: date) -> str:
    return f"outbox_{day:%Y%m%d}"


def ensure_partitions(conn: psycopg.Connection, start: date, days: int) -> None:
    """Create one partition per UTC day in [start, start + days). Safe to rerun."""
    for i in range(days):
        day = start + timedelta(days=i)
        conn.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF outbox FOR VALUES FROM ({}) TO ({})"
        ).format(
            sql.Identifier(partition_name(day)),
            sql.Literal(f"{day} 00:00+00"),
            sql.Literal(f"{day + timedelta(days=1)} 00:00+00"),
        ))


def drop_partition(conn: psycopg.Connection, day: date) -> int:
    """Drop one day's partition if every row in it was published.

    Dead rows are copied to outbox_dead first. Returns the number of rows still
    waiting to be published: 0 means the partition is gone, anything else means
    it was kept.
    """
    part = sql.Identifier(partition_name(day))
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '2s'")  # don't queue writers behind us
        conn.execute(sql.SQL("LOCK TABLE {} IN SHARE MODE").format(part))  # no writes while we check
        pending = conn.execute(sql.SQL(
            "SELECT count(*) FROM {} WHERE published_at IS NULL AND dead_at IS NULL"
        ).format(part)).fetchone()[0]
        if pending:
            return pending
        conn.execute(sql.SQL(
            "INSERT INTO outbox_dead SELECT * FROM {} WHERE dead_at IS NOT NULL"
        ).format(part))
        conn.execute(sql.SQL("DROP TABLE {}").format(part))
    return 0


def drop_expired(conn: psycopg.Connection, today: date, keep_days: int) -> dict[str, int]:
    """Try to drop every partition older than keep_days. Returns {partition: pending rows}."""
    names = [r[0] for r in conn.execute(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = 'outbox'::regclass ORDER BY 1")]
    result = {}
    for name in names:
        day = datetime.strptime(name, "outbox_%Y%m%d").date()
        if day < today - timedelta(days=keep_days):
            result[name] = drop_partition(conn, day)
    return result
