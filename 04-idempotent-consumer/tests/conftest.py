import pathlib
import socket
import sys
import uuid

import psycopg
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import harness  # noqa: E402


@pytest.fixture
def dsn():
    """A fresh schema with the post's tables and 100 empty accounts."""
    schema = "t_" + uuid.uuid4().hex[:10]
    yield harness.setup_schema(schema)
    harness.drop_schema(schema)


@pytest.fixture
def conn(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


def kafka_up() -> bool:
    try:
        socket.create_connection(("localhost", 59092), timeout=1).close()
        return True
    except OSError:
        return False


needs_kafka = pytest.mark.skipif(not kafka_up(), reason="Kafka not reachable on localhost:59092")
