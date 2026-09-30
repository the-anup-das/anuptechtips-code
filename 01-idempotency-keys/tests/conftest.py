"""Fixtures: Postgres database `idem` and Redis DB 1 from ../docker-compose.yml."""
import pathlib

import psycopg
import pytest
import redis

from app import DSN
from helpers import REDIS_URL

SCHEMA = pathlib.Path(__file__).resolve().parent.parent / "schema.sql"


@pytest.fixture(scope="session", autouse=True)
def schema() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA.read_text())


@pytest.fixture
def pg():
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("TRUNCATE idempotency_keys, charges RESTART IDENTITY")
        yield conn


@pytest.fixture
def r():
    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()  # DB 1 belongs to this post
    yield client
    client.close()
