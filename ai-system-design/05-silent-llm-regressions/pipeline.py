"""Probe results travel as events: Kafka topic -> this consumer -> per-slice counters in
Postgres. Kafka delivers at least once, so the counters must not count an event twice.

    python pipeline.py        # consumes regress.events until Ctrl+C, see README.md
"""
import json
import os
import threading

import psycopg
from confluent_kafka import Consumer, Producer
from psycopg.types.json import Jsonb

DSN = os.environ.get("REGRESS_DSN", "postgresql://patterns:patterns@localhost:55432/regress")
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:59092")
TOPIC = "regress.events"

# One statement, so it is atomic: claim the event IDs, then count only the ones that were new.
RECORD = """
WITH fresh AS (
    INSERT INTO processed_events (event_id)
    SELECT (e->>'event_id')::uuid FROM jsonb_array_elements(%(events)s) AS e
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
)
INSERT INTO slice_stats AS s (tick, suite, pool, hardware, batch_bucket, model_version,
                              prompt_version, client_version, total, passed, script_ok)
SELECT e.tick, e.suite, e.pool, e.hardware, e.batch_bucket, e.model_version,
       e.prompt_version, e.client_version,
       count(*), count(*) FILTER (WHERE e.passed), count(*) FILTER (WHERE e.script_ok)
FROM jsonb_to_recordset(%(events)s) AS e(
         event_id uuid, tick int, suite text, pool text, hardware text, batch_bucket text,
         model_version text, prompt_version text, client_version text,
         passed boolean, script_ok boolean)
JOIN fresh USING (event_id)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ON CONFLICT (tick, suite, pool, hardware, batch_bucket,
             model_version, prompt_version, client_version)
DO UPDATE SET total = s.total + EXCLUDED.total,
              passed = s.passed + EXCLUDED.passed,
              script_ok = s.script_ok + EXCLUDED.script_ok
"""


def record(conn: psycopg.Connection, events: list[dict]) -> None:
    """Add a batch of events to the slice counters, skipping any event already counted.
    `conn` is in autocommit mode, so the statement is its own transaction."""
    unique = {e["event_id"]: e for e in events}                # a batch can repeat an event too
    batch = [unique[event_id] for event_id in sorted(unique)]  # same lock order in every consumer
    conn.execute(RECORD, {"events": Jsonb(batch)})


def publish(producer: Producer, events: list[dict], topic: str = TOPIC) -> None:
    for event in events:
        while True:
            try:
                producer.produce(topic, key=event["event_id"], value=json.dumps(event))
                break
            except BufferError:           # the local queue is full: let librdkafka send some
                producer.poll(0.5)
    producer.flush(30)


def consume(topic: str = TOPIC, group: str = "slice-stats", dsn: str = DSN,
            stop: threading.Event | None = None, batch: int = 500) -> None:
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": group,
        "enable.auto.commit": False,      # we commit, and only after Postgres has
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            while not (stop and stop.is_set()):
                msgs = [m for m in consumer.consume(batch, timeout=0.5) if not m.error()]
                if msgs:
                    record(conn, [json.loads(m.value()) for m in msgs])
                    # A crash right here redelivers the batch, and RECORD skips all of it.
                    consumer.commit(asynchronous=False)
        finally:
            consumer.close()


if __name__ == "__main__":
    consume()
