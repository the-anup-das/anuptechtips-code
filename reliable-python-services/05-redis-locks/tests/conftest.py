import os
import pathlib
import sys

import psycopg
import pytest
import redis

HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
PG_DSN = os.environ.get("PG_DSN", "postgresql://patterns:patterns@localhost:55432/locks")
TEST_ROW = 7  # measurements use ids >= 100, the demos use id 1


def _clean(client: redis.Redis) -> None:
    for pattern in ("t:*", "{t:*"):  # every test key starts with one of these
        for key in client.scan_iter(pattern):
            client.delete(key)


@pytest.fixture
def r():
    client = redis.Redis.from_url(REDIS_URL)
    _clean(client)
    yield client
    _clean(client)
    client.close()


@pytest.fixture
def db():
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute((HERE / "schema.sql").read_text())
        conn.execute("INSERT INTO counters (id, value, fence) VALUES (%s, 0, 0) "
                     "ON CONFLICT (id) DO UPDATE SET value = 0, fence = 0", (TEST_ROW,))
        yield conn


@pytest.fixture
def db2():
    """A second, independent connection (another worker)."""
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        yield conn
