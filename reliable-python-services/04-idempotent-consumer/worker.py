"""Run one consumer variant as its own process, so a test can kill it hard.

Variants (the side effect is always one Postgres transaction, so a crash can't half-apply it):
  a1  auto-commit with the defaults, poll() one message at a time
  a2  auto-commit with the defaults, consume() 100-message batches
  b   consume.run() with the inbox insert removed: commit the offset after the DB commit
  c   consume.run() exactly as shown in the post

Options add the test conditions: --work-ms sleeps inside every handler (a stand-in for
real work), --slow-every/--slow-seconds stall one message in N the first time it's handled.
"""
import argparse
import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer

import consume


def plain_once(conn, event_id, event, apply=consume.credit_account) -> bool:
    """Variant b: the same transaction without the inbox row."""
    with conn.transaction():
        apply(conn, event_id, event)
    return True


def run_naive(topic: str, group: str, apply, batch: int | None, **overrides) -> None:
    consumer = Consumer({"bootstrap.servers": consume.BROKERS, "group.id": group,
                         "auto.offset.reset": "earliest", **overrides})  # auto-commit: defaults
    consumer.subscribe([topic])
    with psycopg.connect(consume.DSN, autocommit=True) as conn:
        while True:
            msgs = consumer.consume(batch, 1.0) if batch else [consumer.poll(1.0)]
            for msg in msgs:
                if msg is None or msg.error():
                    continue
                with conn.transaction():
                    apply(conn, consume.event_id_of(msg), json.loads(msg.value()))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", required=True, choices=["a1", "a2", "b", "c"])
    ap.add_argument("--topic", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--work-ms", type=float, default=0)
    ap.add_argument("--slow-every", type=int, default=0)
    ap.add_argument("--slow-seconds", type=float, default=0)
    ap.add_argument("--session-timeout-ms", type=int)
    ap.add_argument("--max-poll-interval-ms", type=int)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    marker = psycopg.connect(os.environ["CONSUMER_DSN"], autocommit=True) if args.slow_every else None

    def apply(conn, event_id, event):
        # one event per block of N is slow: seq 0, N+1, 2N+2, ... (different keys, so
        # the slow ones land on different partitions)
        seq = event["seq"]
        if args.slow_every and seq % args.slow_every == seq // args.slow_every:
            first = marker.execute("INSERT INTO slow_once (event_id) VALUES (%s) "
                                   "ON CONFLICT DO NOTHING RETURNING 1", (event_id,)).fetchone()
            if first:
                logging.info("stalling %.0f s on seq %s", args.slow_seconds, seq)
                time.sleep(args.slow_seconds)
                logging.info("stall over on seq %s", seq)
        if args.work_ms:
            time.sleep(args.work_ms / 1000)
        consume.credit_account(conn, event_id, event)

    overrides = {}
    if args.session_timeout_ms:
        overrides["session.timeout.ms"] = args.session_timeout_ms
    if args.max_poll_interval_ms:
        overrides["max.poll.interval.ms"] = args.max_poll_interval_ms

    if args.variant in ("a1", "a2"):
        run_naive(args.topic, args.group, apply, 100 if args.variant == "a2" else None, **overrides)
    else:
        if args.variant == "b":
            consume.handle_once = plain_once  # run() looks the name up per message
        consume.run(args.topic, args.group, apply=apply, **overrides)


if __name__ == "__main__":
    main()
