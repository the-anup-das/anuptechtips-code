import uuid

import redis
from fastapi.testclient import TestClient

from app import create_app
from redis_limiters import connect


def test_429_with_retry_after_and_ratelimit_headers():
    r = connect()
    client = TestClient(create_app(r))
    headers = {"x-api-key": f"test-{uuid.uuid4().hex}"}
    codes = [client.get("/search", headers=headers).status_code for _ in range(20)]
    assert codes == [200] * 20
    denied = client.get("/search", headers=headers)
    assert denied.status_code == 429
    assert denied.headers["retry-after"] == "1"       # next token in <= 0.6 s, rounded up
    assert denied.headers["ratelimit-policy"] == '"per-key";q=100;w=60'
    assert denied.headers["ratelimit"] == '"per-key";r=0;t=1'
    r.delete(f"rl:tb:{headers['x-api-key']}")


def test_redis_down_fails_open_for_search_and_closed_for_login():
    dead = redis.Redis(host="localhost", port=1, socket_connect_timeout=0.05)
    client = TestClient(create_app(dead))
    assert client.get("/search").status_code == 200
    login = client.post("/login")
    assert login.status_code == 503 and login.headers["retry-after"] == "1"
