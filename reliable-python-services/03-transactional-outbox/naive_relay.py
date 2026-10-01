"""The relay most tutorials stop at. Correct with one copy running; run two and
both read the same pending rows, so both publish them."""
from __future__ import annotations

import psycopg

from relay import Event, Publisher


def naive_relay_batch(conn: psycopg.Connection, publisher: Publisher, batch_size: int = 100) -> int:
    # Instead of this: a plain SELECT, so a second relay sees the same rows
    rows = conn.execute(
        "SELECT id, aggregatetype, aggregateid, type, payload FROM outbox "
        "WHERE published_at IS NULL ORDER BY created_at LIMIT %s", (batch_size,)).fetchall()
    events = [Event(*row) for row in rows]
    if events:
        publisher.publish(events)
        conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                     ([e.id for e in events],))
    return len(events)
