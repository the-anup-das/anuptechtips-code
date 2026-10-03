"""Fixtures: a fresh schema in Postgres database `regress` per test, and Redis DB 10."""
import socket
import uuid

import psycopg
import pytest
import redis

import harness
import rollout


@pytest.fixture
def dsn():
    schema = "t_" + uuid.uuid4().hex[:10]
    yield harness.setup_schema(schema)
    harness.drop_schema(schema)


@pytest.fixture
def conn(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


@pytest.fixture
def r():
    client = redis.Redis.from_url(rollout.REDIS_URL)
    client.flushdb()   # DB 10 belongs to this post
    yield client
    client.close()


def kafka_up() -> bool:
    try:
        socket.create_connection(("localhost", 59092), timeout=1).close()
        return True
    except OSError:
        return False


needs_kafka = pytest.mark.skipif(not kafka_up(), reason="Kafka not reachable on localhost:59092")
