"""Fixtures: the mock provider and the gateway run under uvicorn on free local ports,
and Redis DB 11 (from the series' docker-compose.yml) holds the gateway's state."""
import dataclasses

import pytest
import redis as sync_redis
import redis.asyncio as redis

import harness
import mock_upstream
from config import Provider, Settings
from gateway import create_app
from helpers import REDIS_URL, ROUTES, http


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def mock_url():
    with harness.serve(mock_upstream.app) as url:
        yield url


@pytest.fixture(autouse=True)
def clean(mock_url):
    """Every test starts with an empty Redis DB 11 and healthy mock providers."""
    client = sync_redis.Redis.from_url(REDIS_URL)
    client.flushdb()  # DB 11 belongs to this post
    client.close()
    http.post(f"{mock_url}/_reset")


@pytest.fixture
async def r():
    # A blocking pool, as in the gateway: 200 concurrent commands share 50 connections.
    client = redis.Redis(connection_pool=redis.BlockingConnectionPool.from_url(
        REDIS_URL, decode_responses=True, max_connections=50))
    yield client
    await client.aclose()


@dataclasses.dataclass
class Running:
    url: str
    records: list[dict]  # the usage records the gateway wrote


@pytest.fixture
def gateway(mock_url):
    """Start a gateway with the test routes; keyword arguments override its Settings."""
    stack = []

    def start(routes=ROUTES, **overrides) -> Running:
        providers = {name: Provider(f"{mock_url}/{name}", f"sk-mock-{name}")
                     for name in ("alpha", "beta")}
        settings = Settings(**{"redis_url": REDIS_URL, "providers": providers,
                               "backoff_base_s": 0.01, "idle_gap_s": 0.4, **overrides})
        records: list[dict] = []
        server = harness.serve(create_app(settings, routes, records.append))
        url = server.__enter__()
        stack.append(server)
        return Running(url, records)

    yield start
    for server in stack:
        server.__exit__(None, None, None)
