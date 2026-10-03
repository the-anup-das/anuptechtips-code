"""Shared plumbing for the tests and measurements (not part of the design itself).

Each run gets its own Postgres schema, Kafka topic and consumer group, so runs can't see
each other's rows or offsets.
"""
import pathlib
import threading
import time

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.conninfo import make_conninfo

import pipeline

HERE = pathlib.Path(__file__).resolve().parent
MACHINE = ("AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, "
           "Docker Desktop 28.5 (WSL2)")


def setup_schema(schema: str) -> str:
    """Create `schema` with the post's tables; returns a DSN whose search_path points at it."""
    with psycopg.connect(pipeline.DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
    dsn = make_conninfo(pipeline.DSN, options=f"-c search_path={schema}")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((HERE / "schema.sql").read_text())
    return dsn


def drop_schema(schema: str) -> None:
    with psycopg.connect(pipeline.DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


_ADMIN: AdminClient | None = None


def admin() -> AdminClient:
    global _ADMIN   # one long-lived client: a temporary one dies before its futures resolve
    if _ADMIN is None:
        _ADMIN = AdminClient({"bootstrap.servers": pipeline.BROKERS})
    return _ADMIN


def create_topic(name: str, partitions: int = 3) -> None:
    assert name.startswith("regress."), "this post only touches topics with its own prefix"
    futures = admin().create_topics([NewTopic(name, num_partitions=partitions,
                                              replication_factor=1)])
    futures[name].result(timeout=30)
    for _ in range(100):   # wait until every partition has a leader
        # List all topics: asking for this one by name makes the client track it, and its
        # next metadata refresh would re-create the topic after delete_topic() removed it.
        meta = admin().list_topics(timeout=10).topics.get(name)
        if meta and meta.error is None and len(meta.partitions) == partitions and all(
                p.leader >= 0 for p in meta.partitions.values()):
            return
        time.sleep(0.1)
    raise RuntimeError(f"topic {name} not ready")


def delete_topic(name: str) -> None:
    assert name.startswith("regress."), "this post only touches topics with its own prefix"
    for future in admin().delete_topics([name], operation_timeout=30).values():
        try:
            future.result(timeout=30)
        except Exception:   # already gone
            pass


def start_consumer(topic: str, group: str, dsn: str) -> tuple[threading.Thread, threading.Event]:
    stop = threading.Event()
    thread = threading.Thread(target=pipeline.consume, args=(topic, group, dsn, stop), daemon=True)
    thread.start()
    return thread, stop


def lag(group: str, topic: str) -> int:
    """Messages in `topic` that `group` hasn't committed yet."""
    consumer = Consumer({"bootstrap.servers": pipeline.BROKERS, "group.id": group,
                         "enable.auto.commit": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        behind = 0
        for tp in consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=10):
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            behind += high - (tp.offset if tp.offset >= 0 else low)
        return behind
    finally:
        consumer.close()


def wait_caught_up(group: str, topic: str, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if lag(group, topic) == 0:
            return
        time.sleep(0.3)
    raise TimeoutError(f"group {group} never caught up on {topic}")


def wait_for_events(dsn: str, expected: int, timeout: float = 120) -> None:
    """Block until the consumer has counted `expected` distinct events."""
    deadline = time.time() + timeout
    with psycopg.connect(dsn, autocommit=True) as conn:
        while time.time() < deadline:
            if conn.execute("SELECT count(*) FROM processed_events").fetchone()[0] >= expected:
                return
            time.sleep(0.2)
    raise TimeoutError(f"consumer counted fewer than {expected} events in {timeout}s")
