"""Connection strings and schema setup for the outbox examples.

python db.py   creates the `outbox` database, the tables and a week of partitions.
"""
from __future__ import annotations

import os
import pathlib
from datetime import datetime, timedelta, timezone

import psycopg

from cleanup import ensure_partitions

HERE = pathlib.Path(__file__).resolve().parent
ADMIN_DSN = os.environ.get("OUTBOX_ADMIN_DSN", "postgresql://patterns:patterns@localhost:55432/patterns")
DSN = os.environ.get("OUTBOX_DSN", "postgresql://patterns:patterns@localhost:55432/outbox")
SQLALCHEMY_URL = DSN.replace("postgresql://", "postgresql+psycopg://", 1)
KAFKA = os.environ.get("OUTBOX_KAFKA", "localhost:59092")


def create_database() -> None:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = 'outbox'").fetchone():
            conn.execute("CREATE DATABASE outbox")


def reset_schema(dsn: str = DSN, notify: bool = False, days_back: int = 1, days_ahead: int = 7) -> None:
    """Drop and recreate everything. Tests and measurements start from here."""
    today = datetime.now(timezone.utc).date()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS outbox, outbox_dead, orders CASCADE")
        conn.execute("DROP FUNCTION IF EXISTS outbox_notify() CASCADE")
        conn.execute((HERE / "schema.sql").read_text())
        if notify:
            conn.execute((HERE / "notify.sql").read_text())
        ensure_partitions(conn, today - timedelta(days=days_back), days_back + days_ahead)


if __name__ == "__main__":
    create_database()
    reset_schema()
    print("outbox schema ready")
