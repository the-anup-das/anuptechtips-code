"""A FastAPI endpoint behind the Redis token bucket: 429 + Retry-After when a
key runs dry, and a per-route choice of failing open or closed when Redis
can't answer. Run: uvicorn app:app --port 8002
"""
import math

import redis
from fastapi import Depends, FastAPI, HTTPException, Request

from redis_limiters import connect, lua_source

RATE, BURST = 100 / 60, 20                      # 100 a minute, bursts of 20
POLICY = '"per-key";q=100;w=60'                 # IETF draft-11 RateLimit-Policy


def create_app(r: redis.Redis) -> FastAPI:
    app = FastAPI()
    token_bucket = r.register_script(lua_source("token_bucket"))

    def limit(fail_open: bool):
        def dependency(request: Request) -> None:
            key = "rl:tb:" + request.headers.get("x-api-key", request.client.host)
            try:
                allowed, wait_ms = token_bucket(keys=[key], args=[RATE, BURST, 1])
            except (redis.ConnectionError, redis.TimeoutError):
                if fail_open:
                    return                      # let it through; alert on this
                raise HTTPException(503, "Rate limiter unavailable", headers={"Retry-After": "1"})
            if not allowed:
                retry = max(1, math.ceil(wait_ms / 1000))   # whole seconds (RFC 9110)
                raise HTTPException(429, "Too Many Requests", headers={
                    "Retry-After": str(retry),
                    "RateLimit-Policy": POLICY,
                    "RateLimit": f'"per-key";r=0;t={retry}',
                })
        return dependency

    @app.get("/search", dependencies=[Depends(limit(fail_open=True))])
    def search() -> dict:
        return {"results": []}

    @app.post("/login", dependencies=[Depends(limit(fail_open=False))])
    def login() -> dict:
        return {"ok": True}

    return app


app = create_app(connect(socket_timeout=0.05))  # a slow limiter is worse than none
