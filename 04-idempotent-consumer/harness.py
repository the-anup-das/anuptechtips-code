"""Shared plumbing for the measurement scripts (not part of the pattern itself).

Each run gets its own Postgres schema, Kafka topic and consumer group, so runs can't
see each other's rows or offsets.
"""
import os
import pathlib
import subprocess
import sys
import time
import uuid

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.conninfo import make_conninfo

from publish import BROKERS, make_producer, publish

HERE = pathlib.Path(__file__).resolve().parent
BASE_DSN = "postgresql://patterns:patterns@localhost:55432/consumer"
N_ACCOUNTS = 100
AMOUNT = 100  # cents per event, so the expected total is easy to check


def schema_dsn(schema: str) -> str:
    return make_conninfo(BASE_DSN, options=f"-c search_path={schema}")


def setup_schema(schema: str, extra_sql: str = "") -> str:
    with psycopg.connect(BASE_DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
    dsn = schema_dsn(schema)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((HERE / "schema.sql").read_text())
        conn.execute("INSERT INTO accounts (id) SELECT generate_series(1, %s)", (N_ACCOUNTS,))
        if extra_sql:
            conn.execute(extra_sql)
    return dsn


def drop_schema(schema: str) -> None:
    with psycopg.connect(BASE_DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


_ADMIN: AdminClient | None = None


def admin() -> AdminClient:
    # one long-lived client: a temporary one is destroyed before its futures resolve
    global _ADMIN
    if _ADMIN is None:
        _ADMIN = AdminClient({"bootstrap.servers": BROKERS})
    return _ADMIN


def create_topic(name: str, partitions: int = 6) -> None:
    fut = admin().create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    fut[name].result(timeout=30)
    # wait until metadata shows every partition with a leader
    for _ in range(100):
        md = admin().list_topics(name, timeout=10).topics.get(name)
        if md and md.error is None and len(md.partitions) == partitions and all(
                p.leader >= 0 for p in md.partitions.values()):
            return
        time.sleep(0.1)
    raise RuntimeError(f"topic {name} not ready")


def delete_topics(names: list[str]) -> None:
    if names:
        for fut in admin().delete_topics(names, operation_timeout=30).values():
            try:
                fut.result(timeout=30)
            except Exception:
                pass


def produce_events(topic: str, n: int) -> list[str]:
    """n events with fresh event IDs; payload carries a sequence number."""
    producer = make_producer()
    ids = []
    for seq in range(n):
        event_id = str(uuid.uuid4())
        publish(producer, topic, event_id,
                {"account_id": seq % N_ACCOUNTS + 1, "amount": AMOUNT, "seq": seq})
        ids.append(event_id)
        if seq % 1000 == 999:
            producer.poll(0)
    if producer.flush(30) != 0:
        raise RuntimeError("produce did not finish")
    return ids


def lag(group: str, topic: str) -> int:
    """Messages between the group's committed offsets and the end of the topic."""
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": group, "enable.auto.commit": False})
    try:
        parts = admin().list_topics(topic, timeout=10).topics[topic].partitions
        tps = [TopicPartition(topic, p) for p in parts]
        committed = c.committed(tps, timeout=10)
        total = 0
        for tp in committed:
            _, high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
            done = tp.offset if tp.offset >= 0 else 0
            total += high - done
        return total
    finally:
        c.close()


def effects(dsn: str) -> dict:
    """Count side effects per event ID in the ledger."""
    with psycopg.connect(dsn) as conn:
        rows, distinct, balance = conn.execute(
            "SELECT count(*), count(DISTINCT event_id), "
            "(SELECT coalesce(sum(balance), 0) FROM accounts) FROM ledger").fetchone()
    return {"rows": rows, "distinct": distinct, "duplicates": rows - distinct,
            "balance_total": int(balance)}


def ledger_max_id(dsn: str) -> int:
    with psycopg.connect(dsn) as conn:
        return conn.execute("SELECT coalesce(max(id), 0) FROM ledger").fetchone()[0]


def start_worker(variant: str, topic: str, group: str, dsn: str, log_path: pathlib.Path,
                 *args: str) -> subprocess.Popen:
    env = {**os.environ, "CONSUMER_DSN": dsn, "PYTHONUNBUFFERED": "1"}
    log = open(log_path, "a", encoding="utf-8")
    log.write(f"\n--- start {variant} {time.strftime('%H:%M:%S')} ---\n")
    log.flush()
    return subprocess.Popen(
        [sys.executable, str(HERE / "worker.py"), "--variant", variant, "--topic", topic,
         "--group", group, *args],
        env=env, stdout=log, stderr=subprocess.STDOUT, cwd=HERE)
