"""Readers that remember "the last row I saw" instead of marking rows.

A watermark reader (a CDC-style tailer, a second consumer of the table, a
replay tool) keeps last_seen and asks for rows after it. Ids and timestamps are
assigned at INSERT, but rows become visible at COMMIT, so a naive watermark
skips any row whose transaction commits after a later one.
"""
from __future__ import annotations

from uuid import UUID

import psycopg


class NaiveWatermark:
    """Instead of this: WHERE id > last_seen. Loses rows that commit out of order."""

    def __init__(self) -> None:
        self.last_seen = UUID(int=0)

    def poll(self, conn: psycopg.Connection, limit: int = 100) -> list[UUID]:
        ids = [r[0] for r in conn.execute(
            "SELECT id FROM outbox WHERE id > %s ORDER BY id LIMIT %s",
            (self.last_seen, limit))]
        if ids:
            self.last_seen = ids[-1]
        return ids


class SnapshotWatermark:
    """Use this: only read rows whose transaction ended before every open one began."""

    def __init__(self) -> None:
        self.last_txid, self.last_id = "0", UUID(int=0)  # txid is xid8; keep it as text

    def poll(self, conn: psycopg.Connection, limit: int = 100) -> list[UUID]:
        rows = conn.execute(
            """
            SELECT txid, id FROM outbox
            WHERE (txid, id) > (%s::xid8, %s)
              AND txid < pg_snapshot_xmin(pg_current_snapshot())
            ORDER BY txid, id
            LIMIT %s
            """, (self.last_txid, self.last_id, limit)).fetchall()
        if rows:
            self.last_txid, self.last_id = rows[-1]
        return [r[1] for r in rows]
