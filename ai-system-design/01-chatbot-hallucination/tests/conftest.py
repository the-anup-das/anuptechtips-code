"""Fixtures: Postgres database `chatbot`, Redis DB 6 and Kafka topics `chatbot.audit.test-*`
from the series' docker-compose.yml (see the README)."""
import uuid

import psycopg
import pytest
import redis

from corpus import CLAUSES
from db import DSN, REDIS_URL, create_database, reset
from fake_llm import FakeLLM
from models import Clause
from questions import KEY
from rebuild import create_topic, delete_topics


@pytest.fixture(scope="session", autouse=True)
def database() -> None:
    create_database()


@pytest.fixture
def conn():
    """A fresh schema with version 1 of all 40 clauses."""
    with psycopg.connect(DSN, autocommit=True) as c:
        reset(c)
        yield c


@pytest.fixture
def r():
    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()  # DB 6 belongs to this post
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def topic():
    name = f"chatbot.audit.test-{uuid.uuid4().hex[:8]}"
    create_topic(name)
    yield name
    delete_topics([name])


@pytest.fixture
def clause():
    """Clauses by ID, built without the database, for the pure-Python checks."""
    table = {c.clause_id: Clause(c.clause_id, 1, c.policy_type, c.body, 1.0) for c in CLAUSES}
    return table.__getitem__


def model(mode: str | None = None, seed: int = 1, **kw) -> FakeLLM:
    """The stand-in model, optionally forced into one failure mode."""
    return FakeLLM(seed, KEY, force=mode, **kw)
