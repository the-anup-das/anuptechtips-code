"""Outbox relay: publish the audit events to Kafka, at least once.

Claim a batch with FOR UPDATE SKIP LOCKED, publish it, wait for the broker's acks, then
mark the rows. A crash before the mark rolls the claim back, so the batch is sent again
by the next relay: duplicates are possible, losses are not.

    python relay.py                      run until stopped
    python relay.py --until-empty        drain the outbox and exit
    python relay.py --crash-after 3      for the tests: die after the 3rd batch is acked,
                                         before it is marked
"""
import argparse
import json
import os
import time

import psycopg
from confluent_kafka import Producer

from db import DSN, KAFKA

CLAIM = """
SELECT id, topic, key, payload FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT %s
FOR UPDATE SKIP LOCKED
"""


def make_producer(bootstrap: str = KAFKA) -> Producer:
    return Producer({"bootstrap.servers": bootstrap,
                     "enable.idempotence": True,    # acks=all; retries can't reorder a key
                     "linger.ms": 5})


def relay_batch(conn: psycopg.Connection, producer: Producer, batch_size: int = 50,
                before_mark=lambda: None) -> int:
    """Publish one batch inside one transaction. Returns how many rows it sent."""
    errors = []
    with conn.transaction():
        rows = conn.execute(CLAIM, (batch_size,)).fetchall()
        if not rows:
            return 0
        for outbox_id, topic, key, payload in rows:
            producer.produce(topic, key=key, value=json.dumps(payload),
                             headers={"outbox_id": str(outbox_id)},
                             on_delivery=lambda err, msg: err and errors.append(err))
        if producer.flush(10) or errors:            # anything unacked: roll back, retry later
            raise RuntimeError(f"Kafka did not acknowledge the batch: {errors[:1]}")
        before_mark()
        conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                     ([row[0] for row in rows],))
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--batch", type=int, default=50)
    p.add_argument("--until-empty", action="store_true")
    p.add_argument("--crash-after", type=int, default=0)
    args = p.parse_args()

    producer = make_producer()
    batches = 0

    def maybe_crash() -> None:
        if args.crash_after and batches + 1 == args.crash_after:
            os._exit(1)     # the broker has the batch; Postgres never hears about it

    with psycopg.connect(DSN, autocommit=True) as conn:
        while True:
            sent = relay_batch(conn, producer, args.batch, maybe_crash)
            batches += bool(sent)
            if not sent:
                if args.until_empty:
                    return
                time.sleep(0.1)


if __name__ == "__main__":
    main()
