"""Fixtures: every test gets its own schema in database `rag` on the pgvector container
(../docker-compose.yml, port 55433), with the post's tables in it."""
import uuid

import pytest

import harness
from embedder import HashingEmbedder
from retrieval import connect


@pytest.fixture
def dsn():
    schema = "t_" + uuid.uuid4().hex[:12]
    yield harness.setup_schema(schema)
    harness.drop_schema(schema)


@pytest.fixture
def conn(dsn):
    with connect(dsn) as c:
        yield c


@pytest.fixture
def embed():
    return HashingEmbedder()


@pytest.fixture
def golden_index(conn, embed):
    """The handbook corpus in golden/, indexed with the real chunker and consumer."""
    harness.index_corpus(conn, embed)
    return conn
