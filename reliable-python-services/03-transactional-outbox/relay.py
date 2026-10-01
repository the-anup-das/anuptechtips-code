"""Outbox relay: claim and mark a batch with FOR UPDATE SKIP LOCKED, publish it, commit.

Run as many copies as you like. Each one takes rows the others haven't locked,
so there's no leader election and no distributed lock. A crash rolls the
transaction back, the rows unlock, and another worker publishes them again:
at-least-once delivery.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Protocol, TextIO
from uuid import UUID

import psycopg
from confluent_kafka import KafkaError, KafkaException, Producer

MAX_ATTEMPTS = 10

# Claim and mark in one statement. The mark stays invisible until COMMIT, which
# only happens after the broker acked; a crash or error before that undoes it.
CLAIM = """
    WITH batch AS (
        SELECT id, created_at FROM outbox
        WHERE published_at IS NULL AND dead_at IS NULL AND next_attempt_at <= now()
        ORDER BY next_attempt_at
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox o SET published_at = now()
    FROM batch
    WHERE o.id = batch.id AND o.created_at = batch.created_at
    RETURNING o.id, o.aggregatetype, o.aggregateid, o.type, o.payload
"""

MARK_FAILED = """
    UPDATE outbox
    SET published_at = NULL,
        attempts = attempts + 1,
        last_error = %s,
        next_attempt_at = now() + least(2 ^ attempts, 300) * interval '1 second',
        dead_at = CASE WHEN attempts + 1 >= %s THEN now() END
    WHERE id = %s
"""

# If a relay hangs mid-batch, Postgres ends its session and releases the locks.
SESSION_OPTIONS = "-c idle_in_transaction_session_timeout=30s -c statement_timeout=10s"

log = logging.getLogger("outbox.relay")


@dataclass(frozen=True)
class Event:
    id: UUID
    aggregatetype: str
    aggregateid: str
    type: str
    payload: dict


class Publisher(Protocol):
    def publish(self, events: list[Event]) -> dict[UUID, str]:
        """Send the batch and wait for the broker's acks.

        Return {event id: error} for events the broker refused. Raise if the
        broker can't be reached: the whole batch then rolls back untouched.
        """
        ...


def relay_batch(conn: psycopg.Connection, publisher: Publisher, batch_size: int = 100) -> int:
    """Claim, publish and commit one batch in one transaction. Returns rows claimed."""
    with conn.transaction():
        rows = conn.execute(CLAIM, (batch_size,)).fetchall()
        events = sorted((Event(*row) for row in rows), key=lambda e: e.id)  # uuidv7: insert order
        if events:
            errors = publisher.publish(events)  # blocks until acked; row locks held
            for event_id, error in errors.items():
                conn.execute(MARK_FAILED, (error[:1000], MAX_ATTEMPTS, event_id))
    return len(events)


class TokenBucket:
    """Caps the publish rate, so a backlog after an outage doesn't flood the broker."""

    def __init__(self, rate: float, burst: float):
        self.rate, self.burst = rate, burst
        self.tokens, self.last = burst, time.monotonic()

    def take(self, n: float) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            time.sleep((n - self.tokens) / self.rate)


def run(dsn: str, publisher: Publisher, *, batch_size: int = 100, poll_interval: float = 0.1,
        max_per_second: float | None = None, stop: Callable[[], bool] = lambda: False) -> None:
    """Polling publisher: drain the table, sleep poll_interval, repeat."""
    bucket = TokenBucket(max_per_second, burst=batch_size) if max_per_second else None
    while not stop():
        try:
            with psycopg.connect(dsn, autocommit=True, options=SESSION_OPTIONS) as conn:
                while not stop():
                    claimed = relay_batch(conn, publisher, batch_size)
                    if bucket and claimed:
                        bucket.take(claimed)
                    if claimed < batch_size:
                        time.sleep(poll_interval)
        except Exception:
            log.exception("batch failed; its rows stay pending")
            time.sleep(1.0)  # broker or database down: back off, then reconnect


class PublishError(Exception):
    """The broker didn't ack the batch in time. Nothing gets marked."""


class KafkaPublisher:
    """Topic outbox.event.<aggregatetype> and key aggregateid, like Debezium's router."""

    def __init__(self, bootstrap_servers: str, topic_prefix: str = "outbox.event.",
                 timeout: float = 10.0):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,  # acks=all; producer retries can't duplicate or reorder
            "linger.ms": 5,
            "message.timeout.ms": int(timeout * 1000),  # give up when the relay does
        })
        self.topic_prefix, self.timeout = topic_prefix, timeout

    def publish(self, events: list[Event]) -> dict[UUID, str]:
        acked: set[UUID] = set()
        refused: dict[UUID, str] = {}

        def on_delivery(err: KafkaError | None, event_id: UUID) -> None:
            if err is None:
                acked.add(event_id)
            elif not err.retriable() and err.code() != KafkaError._MSG_TIMED_OUT:
                refused[event_id] = err.str()  # the broker rejected this message

        for e in events:
            try:
                self.producer.produce(
                    self.topic_prefix + e.aggregatetype,
                    key=e.aggregateid,
                    value=json.dumps(e.payload),
                    headers={"id": str(e.id), "type": e.type},
                    on_delivery=lambda err, msg, event_id=e.id: on_delivery(err, event_id),
                )
            except KafkaException as exc:  # e.g. MSG_SIZE_TOO_LARGE, before it's sent
                refused[e.id] = str(exc)
        self.producer.flush(self.timeout + 1)  # every ack arrives before the caller commits
        missing = [e.id for e in events if e.id not in acked and e.id not in refused]
        if missing:
            raise PublishError(f"{len(missing)} of {len(events)} events not acked")
        return refused


class StubPublisher:
    """Stands in for a broker: waits about 2 ms per batch and records what it 'sent'.

    With log= a file, the ids survive a kill -9 of this process, so a test can
    count duplicates afterwards.
    """

    def __init__(self, delay: float = 0.002, log: TextIO | None = None):
        self.delay, self.log = delay, log
        self.sent: list[UUID] = []

    def publish(self, events: list[Event]) -> dict[UUID, str]:
        time.sleep(self.delay)
        self.sent.extend(e.id for e in events)
        if self.log:
            self.log.write("".join(f"{e.id}\n" for e in events))
            self.log.flush()
        return {}
