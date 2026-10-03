"""Rebuild the audit trail from Kafka alone, as a dispute handler or a new consumer would,
and compare it with the answer_audit table.

    python rebuild.py chatbot.audit
"""
import json
import sys
import time

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from db import DSN, KAFKA

FIELDS = ("conversation_id", "question", "intent", "action", "stage", "draft", "reply",
          "cited", "checks")


def read_topic(topic: str, bootstrap: str = KAFKA, timeout: float = 30.0) -> list[dict]:
    """Every event in the topic right now, read from the start of each partition."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "chatbot-rebuild",
                         "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        ends, assignment = {}, []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                ends[p] = high
                assignment.append(TopicPartition(topic, p, OFFSET_BEGINNING))
        consumer.assign(assignment)
        events, deadline = [], time.monotonic() + timeout
        while ends and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            events.append(json.loads(msg.value()))
            if msg.offset() + 1 >= ends.get(msg.partition(), 0):
                ends.pop(msg.partition(), None)     # reached the end of this partition
        return events
    finally:
        consumer.close()


def rebuild(events: list[dict]) -> dict[str, dict]:
    """One record per audit_id. Delivery is at-least-once, so the same event can arrive
    twice; keying on its id makes the second copy a no-op."""
    return {event["audit_id"]: event for event in events}


def conversation(trail: dict[str, dict], conversation_id: str) -> list[dict]:
    """One conversation's replies in order (uuidv7 ids sort by time)."""
    return sorted((e for e in trail.values() if e["conversation_id"] == conversation_id),
                  key=lambda e: e["audit_id"])


def mismatches(conn: psycopg.Connection, trail: dict[str, dict]) -> list[str]:
    """Audit rows that are missing from the rebuilt trail or differ from it."""
    bad = []
    rows = conn.execute(f"SELECT id::text, {', '.join(FIELDS)} FROM answer_audit").fetchall()
    for audit_id, *values in rows:
        event = trail.get(audit_id)
        if event is None or [event[f] for f in FIELDS] != values:
            bad.append(audit_id)
    return bad


def create_topic(topic: str, bootstrap: str = KAFKA, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})     # must outlive its futures
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for future in futures.values():
        future.result(timeout=15)


def delete_topics(topics: list[str], bootstrap: str = KAFKA) -> None:
    if topics:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        for future in admin.delete_topics(topics).values():
            future.result(timeout=15)


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "chatbot.audit"
    events = read_topic(topic)
    trail = rebuild(events)
    with psycopg.connect(DSN, autocommit=True) as conn:
        bad = mismatches(conn, trail)
    print(f"{len(events)} events, {len(trail)} replies, {len(events) - len(trail)} duplicates, "
          f"{len(bad)} mismatches with answer_audit")
