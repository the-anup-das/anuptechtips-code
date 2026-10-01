"""LISTEN/NOTIFY as a wake-up call for the relay. The table stays the source of truth.

Needs notify.sql applied, and a direct connection for LISTEN: PgBouncer in
transaction pooling mode doesn't support it.
"""
from __future__ import annotations

import time
from typing import Callable

import psycopg

from relay import SESSION_OPTIONS, Publisher, log, relay_batch


def run_with_notify(dsn: str, publisher: Publisher, *, batch_size: int = 100,
                    fallback_poll: float = 1.0, stop: Callable[[], bool] = lambda: False) -> None:
    while not stop():
        try:
            with psycopg.connect(dsn, autocommit=True) as listener, \
                 psycopg.connect(dsn, autocommit=True, options=SESSION_OPTIONS) as conn:
                listener.execute("LISTEN outbox")
                while not stop():
                    # Drain first: rows committed while nobody listened sent no NOTIFY.
                    while relay_batch(conn, publisher, batch_size) == batch_size:
                        pass
                    # Sleep until a commit notifies us, or fallback_poll seconds pass.
                    # A NOTIFY that arrived during the drain is returned at once.
                    for _ in listener.notifies(timeout=fallback_poll, stop_after=1):
                        pass
        except Exception:
            log.exception("relay failed; reconnecting")
            time.sleep(1.0)
