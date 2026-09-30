"""An idempotent Kafka consumer: the inbox row and the side effect commit in one
Postgres transaction, and the Kafka offset is committed after that.

    python consume.py        # reads consumer.deposits, see README.md
"""
import json
import logging
import os

import psycopg
from confluent_kafka import Consumer, KafkaException, Message, Producer

DSN = os.environ.get("CONSUMER_DSN", "postgresql://patterns:patterns@localhost:55432/consumer")
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:59092")
CONSUMER_NAME = "wallet-credits"  # scopes the inbox: every consumer dedups on its own
log = logging.getLogger("consume")


def credit_account(conn: psycopg.Connection, event_id: str, event: dict) -> None:
    """The side effect. It's an increment, so running it twice credits twice."""
    conn.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                 (event["amount"], event["account_id"]))
    conn.execute("INSERT INTO ledger (event_id, account_id, amount) VALUES (%s, %s, %s)",
                 (event_id, event["account_id"], event["amount"]))


def handle_once(conn: psycopg.Connection, event_id: str, event: dict, apply=credit_account) -> bool:
    """Apply the event unless this consumer already has. Returns True if it ran."""
    with conn.transaction():
        claimed = conn.execute(
            "INSERT INTO processed_messages (consumer, message_id) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING RETURNING message_id",
            (CONSUMER_NAME, event_id),
        ).fetchone()
        if claimed is None:
            return False  # already done: skip it, but still commit the offset
        apply(conn, event_id, event)  # an exception here rolls back the inbox row too
    return True


def event_id_of(msg: Message) -> str | None:
    for key, value in msg.headers() or []:
        if key == "event_id" and value:
            return value.decode()
    return None


def run(topic: str, group: str, apply=credit_account, **overrides) -> None:
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": group,
        "enable.auto.commit": False,      # we commit, and only after Postgres has
        "auto.offset.reset": "earliest",  # a new group starts at the beginning, not the end
        **overrides,
    })
    dead_letters = Producer({"bootstrap.servers": BROKERS})
    consumer.subscribe([topic])
    with psycopg.connect(DSN, autocommit=True) as conn:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("consumer error: %s", msg.error())
                    continue
                event_id = event_id_of(msg)
                if event_id is None:  # nothing to dedup on, so park it instead of guessing
                    dead_letters.produce(topic + ".dlq", value=msg.value(), key=msg.key(),
                                         headers=msg.headers())
                    dead_letters.flush()
                elif not handle_once(conn, event_id, json.loads(msg.value()), apply):
                    log.info("skipped duplicate %s", event_id)
                try:
                    consumer.commit(message=msg, asynchronous=False)  # last, after the DB commit
                except KafkaException as e:
                    # We lost this partition in a rebalance. Its new owner will get the
                    # message again and skip it, because the inbox row is already there.
                    log.warning("offset commit failed: %s", e)
        finally:
            consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run("consumer.deposits", group=CONSUMER_NAME)
