"""A rate limiter that tells the truth when its own store is down: a 503 or a local fallback,
never a 429 for a client that did nothing wrong. Run: uvicorn limiter:app --port 8003
"""
import math
import pathlib

import redis
from fastapi import Depends, FastAPI, HTTPException, Request
from redis.backoff import NoBackoff
from redis.retry import Retry

from lab import REDIS_URL
from token_bucket import TokenBucket

RATE, BURST = 100 / 60, 20                      # 100 a minute, bursts of 20
INSTANCES = 4                                   # API instances that share the limit
LUA = (pathlib.Path(__file__).parent / "lua" / "token_bucket.lua").read_text()


def connect(url: str = REDIS_URL, timeout: float = 0.05) -> redis.Redis:
    """A limiter store that gives up fast: both timeouts set, and no retries."""
    return redis.Redis.from_url(url, socket_timeout=timeout, socket_connect_timeout=timeout,
                                retry=Retry(NoBackoff(), 0))


def create_app(r: redis.Redis) -> FastAPI:
    app = FastAPI()
    token_bucket = r.register_script(LUA)
    local: dict[str, TokenBucket] = {}          # fallback buckets, used only while Redis is down

    # Instead of this: any limiter failure turns into "you sent too many requests"
    def naive_limit(request: Request) -> None:
        key = "rl:tb:" + request.headers.get("x-api-key", request.client.host)
        try:
            allowed, wait_ms = token_bucket(keys=[key], args=[RATE, BURST, 1])
        except Exception:
            allowed = 0                         # "deny, to be safe"
        if not allowed:
            raise HTTPException(429, "Too Many Requests", headers={"Retry-After": "1"})

    # Use this: a broken limiter is the server's problem, and the status code says so
    def limit(fail_open: bool):
        def dependency(request: Request) -> None:
            key = "rl:tb:" + request.headers.get("x-api-key", request.client.host)
            try:
                allowed, wait_ms = token_bucket(keys=[key], args=[RATE, BURST, 1])
            except (redis.ConnectionError, redis.TimeoutError):
                if not fail_open:
                    raise HTTPException(503, "Rate limiter unavailable",
                                        headers={"Retry-After": "1"})
                if key not in local:            # fail open, on this instance's share of the limit
                    local[key] = TokenBucket(RATE / INSTANCES, BURST / INSTANCES)
                wait_ms = local[key].try_acquire() * 1000
                allowed = wait_ms == 0
            if not allowed:
                retry = max(1, math.ceil(wait_ms / 1000))   # whole seconds (RFC 9110)
                raise HTTPException(429, "Too Many Requests", headers={"Retry-After": str(retry)})
        return dependency

    @app.get("/search", dependencies=[Depends(limit(fail_open=True))])
    def search() -> dict:
        return {"results": []}

    @app.post("/login", dependencies=[Depends(limit(fail_open=False))])
    def login() -> dict:
        return {"ok": True}

    @app.get("/search-naive", dependencies=[Depends(naive_limit)])
    def search_naive() -> dict:
        return {"results": []}

    return app


app = create_app(connect())
