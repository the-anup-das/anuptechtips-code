"""Where the lab's services live, and the few names every script shares.

PostgreSQL, Redis and Kafka are the Docker services from the repo's
reliable-python-services/docker-compose.yml. This post uses its own database
(failmodes), Redis DB 8 and Kafka topics that start with "failmodes.".
"""
import os
import pathlib
import time

import psycopg
import redis
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ADMIN_DSN = os.environ.get("FAILMODES_ADMIN_DSN",
                           "postgresql://patterns:patterns@localhost:55432/patterns")
DSN = os.environ.get("FAILMODES_DSN", "postgresql://patterns:patterns@localhost:55432/failmodes")
REDIS_URL = os.environ.get("FAILMODES_REDIS_URL", "redis://localhost:56379/8")
KAFKA = os.environ.get("FAILMODES_KAFKA", "localhost:59092")
# Every Kafka client in the lab starts from this. The address family is pinned because
# "localhost" can also mean ::1, and on Docker Desktop for Windows a connection over ::1 to
# the published port added about 50 ms to every produce of a 1 KB file.
KAFKA_CLIENT = {"bootstrap.servers": KAFKA, "broker.address.family": "v4"}

SCHEMA = pathlib.Path(__file__).resolve().parent / "schema.sql"
COHORTS = (1, 3, 8)        # proxies per cohort: one canary, then three, then the other eight


def connect() -> psycopg.Connection:
    """A connection to the lab's database. The database is created on first use."""
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = 'failmodes'").fetchone():
            admin.execute("CREATE DATABASE failmodes")
    return psycopg.connect(DSN, autocommit=True)


def reset_schema(conn: psycopg.Connection) -> None:
    """Create the tables and roles, and put every grant back to "before the change"."""
    conn.execute(SCHEMA.read_text())


def redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL)


def topic(cohort: int) -> str:
    """Each cohort of proxies reads its feature files from its own topic."""
    return f"failmodes.features.cohort-{cohort}"


def health_key(run: str, cohort: int, version: int) -> str:
    """Redis hash with one cohort's request counters for one config version."""
    return f"failmodes:{run}:c{cohort}:v{version}"


def topic_ends() -> dict[int, int]:
    """Create the cohort topics if they are missing. Returns {cohort: end offset}, so a
    proxy can start reading at "now" and skip the files of earlier runs."""
    admin = AdminClient(KAFKA_CLIENT)
    existing = admin.list_topics(timeout=10).topics
    missing = [NewTopic(topic(c), num_partitions=1, replication_factor=1)
               for c in range(len(COHORTS)) if topic(c) not in existing]
    if missing:
        for future in admin.create_topics(missing).values():
            future.result()
    probe = Consumer({**KAFKA_CLIENT, "group.id": "failmodes.offsets"})
    ends = {}
    for cohort in range(len(COHORTS)):
        for _ in range(50):                      # a new topic needs a moment to elect a leader
            try:
                ends[cohort] = probe.get_watermark_offsets(
                    TopicPartition(topic(cohort), 0), timeout=5)[1]
                break
            except KafkaException:
                time.sleep(0.2)
        else:
            raise RuntimeError(f"no leader for {topic(cohort)}")
    probe.close()
    return ends
