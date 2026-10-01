"""The benchmark app for measure_race.py. It serves the post's own endpoint unchanged,
plus the same endpoint on the two other claim strategies:

    POST /charges                  run_once: INSERT ... ON CONFLICT (app.py, as in the post)
    POST /race/check-then-insert   naive.check_then_insert: SELECT, charge, INSERT
    POST /race/redis-set-nx        redis_claim.run_once_redis: SET NX claim

All three do the same work: app.charge, a 50 ms pause plus one row in `charges`.

Two benchmark-only changes, so the race measures the claim rather than the plumbing:
- connections come from a pool opened at startup, not one connect per request;
- AnyIO's worker-thread limit goes from 40 to RACE_THREADS (default 200), so 100 sync
  requests really run at the same time.

Run: uvicorn race_app:app --port 58101
"""
import asyncio
import os
import queue
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager

import anyio.to_thread
import psycopg
import redis
from fastapi import Depends, FastAPI, Header
from fastapi.responses import JSONResponse

import app as base
from idempotency import fingerprint
from naive import check_then_insert
from redis_claim import run_once_redis

THREADS = int(os.environ.get("RACE_THREADS", "200"))
POOL_SIZE = 150
REDIS_URL = os.environ.get("IDEM_REDIS_URL", "redis://localhost:56379/1")

pool: queue.Queue[psycopg.Connection] = queue.Queue()
rds = redis.Redis.from_url(REDIS_URL, max_connections=THREADS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    anyio.to_thread.current_default_thread_limiter().total_tokens = THREADS
    for _ in range(POOL_SIZE):
        pool.put(psycopg.connect(base.DSN, autocommit=True))
    yield
    while not pool.empty():
        pool.get().close()


def pooled_conn() -> Iterator[psycopg.Connection]:
    conn = pool.get()  # blocks while all connections are busy
    try:
        yield conn
    finally:
        pool.put(conn)


app = FastAPI(lifespan=lifespan)
app.include_router(base.app.router)  # POST /charges exactly as in app.py
app.dependency_overrides[base.get_conn] = pooled_conn


@app.get("/health")
async def health(sleep: float = 0) -> dict:
    await asyncio.sleep(sleep)  # measure_race.py uses this to open keep-alive connections
    return {"ok": True, "threads": THREADS}


@app.get("/warm-threads")
def warm_threads(sleep: float = 0.3) -> dict:
    time.sleep(sleep)  # AnyIO starts worker threads lazily; a burst of these starts them
    return {"ok": True}


@app.post("/race/check-then-insert")
def naive_charge(
    body: base.ChargeIn,
    client_id: str = Header(alias="X-Client-Id"),
    idempotency_key: str | None = Header(default=None),
    conn: psycopg.Connection = Depends(pooled_conn),
) -> JSONResponse:
    key = base.parse_key(idempotency_key)
    fp = fingerprint("POST", "/charges", body.model_dump())
    result = check_then_insert(conn, client_id, key, fp,
                               lambda c: base.charge(c, client_id, body.amount, body.currency))
    return base.to_response(result)


@app.post("/race/redis-set-nx")
def redis_charge(
    body: base.ChargeIn,
    client_id: str = Header(alias="X-Client-Id"),
    idempotency_key: str | None = Header(default=None),
    conn: psycopg.Connection = Depends(pooled_conn),
) -> JSONResponse:
    key = base.parse_key(idempotency_key)
    fp = fingerprint("POST", "/charges", body.model_dump())
    result = run_once_redis(rds, client_id, key, fp,
                            lambda: base.charge(conn, client_id, body.amount, body.currency))
    return base.to_response(result)
