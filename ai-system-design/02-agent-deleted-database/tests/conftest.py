"""Fixtures: Postgres database `agentdel`, Redis DB 7 and Kafka topics `agentdel.*` on the
repo's Docker services. The lab's roles are created for the session and dropped at the end."""
import uuid

import psycopg
import pytest
import redis
from confluent_kafka.admin import AdminClient, NewTopic

import lab

SMALL = {"courses_answer": 19_432, "reservations": 5_000}  # 1% and 10% of the full sizes


@pytest.fixture(scope="session", autouse=True)
def roles():
    lab.create()
    yield
    lab.drop()


@pytest.fixture
def db():
    """Fresh schemas with the Replit tables in prod and an inbox in staging."""
    lab.reset(prod=("executives", "companies"), staging=("inbox",))


@pytest.fixture
def r():
    client = redis.Redis.from_url(lab.REDIS_URL)
    client.flushdb()  # DB 7 belongs to this post
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def connect():
    """connect(role) -> a connection that is closed when the test ends."""
    opened: list[psycopg.Connection] = []

    def _connect(role: str = lab.OWNER) -> psycopg.Connection:
        opened.append(lab.connect(role))
        return opened[-1]
    yield _connect
    for conn in opened:
        conn.close()


@pytest.fixture
def topic():
    """A one-partition topic under the lab's prefix, deleted afterwards."""
    name = f"{lab.TOPIC_PREFIX}test-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": lab.KAFKA})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(10)
    yield name
    admin.delete_topics([name])[name].result(10)
