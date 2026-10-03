"""Fixtures. The Redis tests use DB 9 on localhost:56379 and nothing else."""
import pytest
import redis

from helpers import REDIS_DB, REDIS_HOST, REDIS_PORT


@pytest.fixture
def db():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    client.flushdb()  # DB 9 belongs to this post
    yield client
    client.flushdb()
    client.close()
