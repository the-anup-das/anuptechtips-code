import pathlib
import sys
import time

import psycopg
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db import DSN, create_database, reset_schema  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def database():
    create_database()


@pytest.fixture
def dsn():
    reset_schema()
    return DSN


@pytest.fixture
def conn(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


def pending(conn) -> int:
    return conn.execute(
        "SELECT count(*) FROM outbox WHERE published_at IS NULL AND dead_at IS NULL").fetchone()[0]


def claimable(conn, n: int = 1000) -> int:
    """How many rows a relay could claim right now. Claims them, then rolls back."""
    from relay import CLAIM
    with conn.transaction(force_rollback=True):
        return len(conn.execute(CLAIM, (n,)).fetchall())


def wait_until(check, timeout: float = 10.0, every: float = 0.02):
    """Poll check() until it returns something truthy; return it (or the last falsy value)."""
    deadline = time.monotonic() + timeout
    while True:
        value = check()
        if value or time.monotonic() > deadline:
            return value
        time.sleep(every)
