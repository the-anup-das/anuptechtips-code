"""What the rate limiter answers when its own Redis is healthy, refusing connections, or stuck."""
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from limiter import BURST, INSTANCES, connect, create_app
from tarpit import Tarpit, refused_url


def key() -> dict[str, str]:
    return {"x-api-key": f"test-{uuid.uuid4().hex}"}


@pytest.fixture
def healthy(r) -> TestClient:
    return TestClient(create_app(connect()))


@pytest.fixture(params=["refused", "hung"])
def broken(request):
    """The app with its limiter store down: once refusing connections, once not answering."""
    if request.param == "refused":
        yield TestClient(create_app(connect(refused_url())))
    else:
        with Tarpit() as tarpit:
            yield TestClient(create_app(connect(tarpit.url)))


@pytest.mark.parametrize("path", ["/search", "/search-naive"])
def test_a_healthy_limiter_allows_the_burst_then_says_429(healthy, path):
    headers = key()
    assert [healthy.get(path, headers=headers).status_code for _ in range(BURST)] == [200] * BURST
    denied = healthy.get(path, headers=headers)
    assert denied.status_code == 429 and denied.headers["retry-after"] == "1"


def test_the_naive_limiter_blames_the_client_when_redis_is_down(broken):
    first = broken.get("/search-naive", headers=key())    # a client's very first request
    assert first.status_code == 429
    assert first.json() == {"detail": "Too Many Requests"}


def test_the_closed_route_says_503_and_when_to_come_back(broken):
    login = broken.post("/login", headers=key())
    assert login.status_code == 503
    assert login.headers["retry-after"] == "1"
    assert login.json() == {"detail": "Rate limiter unavailable"}


def test_the_open_route_serves_from_this_instances_share(broken):
    headers = key()
    share = BURST // INSTANCES                             # 5 of the 20
    codes = [broken.get("/search", headers=headers).status_code for _ in range(share + 1)]
    assert codes == [200] * share + [429]                  # a real 429: the local share is used up
    assert broken.get("/search", headers=key()).status_code == 200   # other clients are fine


def test_a_dead_limiter_costs_one_timeout_not_seconds(broken):
    started = time.perf_counter()
    broken.post("/login", headers=key())
    assert time.perf_counter() - started < 0.5             # 50 ms timeouts, no retries
