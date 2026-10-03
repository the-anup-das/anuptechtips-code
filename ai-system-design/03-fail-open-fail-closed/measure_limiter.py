"""M6: what a client sees when the rate limiter's own Redis is down, and how long it waits.

Statuses: the first request of a fresh client on each route, with the limiter's Redis
healthy, refusing connections, and accepting connections but never answering ("hung").

Latency: the time one POST /login takes in each state, for three ways to set up the client:
  both timeouts, no retries   socket_timeout and socket_connect_timeout at 50 ms, retries off
                              (limiter.connect, what this folder's app uses)
  both timeouts               redis.Redis(host, port, ...) with the same two timeouts and
                              redis-py's default retries
  socket_timeout only         redis.Redis(host, port, socket_timeout=0.05) and nothing else

Requests go through FastAPI's TestClient, in-process, so there is no HTTP network hop.

Writes results/m6_limiter_status.csv, m6_limiter_latency.csv and m6_limiter_summary.json.
"""
import csv
import inspect
import json
import os
import pathlib
import statistics
import sys
import time
import urllib.parse
import uuid

import redis
from fastapi.testclient import TestClient

import lab
from limiter import connect, create_app
from tarpit import Tarpit, refused_url

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))                 # the series folder
from common.measure_lock import measuring  # noqa: E402

ROUNDS = int(os.environ.get("RUNS", "5"))
FAST, SLOW = 100, 1                                  # requests per round
ROUTES = [("naive limiter", "GET", "/search-naive"),
          ("fail open (/search)", "GET", "/search"),
          ("fail closed (/login)", "POST", "/login")]


def by_constructor(url: str, **kwargs) -> redis.Redis:
    """redis.Redis(host=..., port=...): the form that gets redis-py's default retries.
    (Redis.from_url builds its own pool, and that path has no retries by default.)"""
    parts = urllib.parse.urlparse(url)
    return redis.Redis(host=parts.hostname, port=parts.port, db=int(parts.path[1:] or 0),
                       **kwargs)


CLIENTS = {
    "both timeouts, no retries": lambda url: connect(url),
    "both timeouts": lambda url: by_constructor(url, socket_timeout=0.05,
                                                socket_connect_timeout=0.05),
    "socket_timeout only": lambda url: by_constructor(url, socket_timeout=0.05),
}


def key() -> dict[str, str]:
    return {"x-api-key": f"m6-{uuid.uuid4().hex}"}


def statuses(state: str, url: str) -> list[dict]:
    client = TestClient(create_app(connect(url)))
    rows = []
    for name, method, path in ROUTES:
        response = client.request(method, path, headers=key())
        rows.append({"limiter_redis": state, "route": name, "status": response.status_code,
                     "retry_after": response.headers.get("retry-after", ""),
                     "detail": response.json().get("detail", "")})
    return rows


def latency(state: str, url: str, client_name: str, requests: int) -> list[dict]:
    client = TestClient(create_app(CLIENTS[client_name](url)))
    rows = []
    for round_no in range(1, ROUNDS + 1):
        for _ in range(requests):
            headers = key()
            started = time.perf_counter()
            status = client.post("/login", headers=headers).status_code
            rows.append({"limiter_redis": state, "client": client_name, "round": round_no,
                         "status": status,
                         "ms": round((time.perf_counter() - started) * 1000, 2)})
    return rows


def main() -> None:
    lab.redis_client().flushdb()
    defaults = redis.Redis().get_retry()
    signature = inspect.signature(redis.Redis.__init__).parameters
    status_rows, latency_rows = [], []
    # a short poll, so a queue of other benchmarks on this machine can't starve this one
    with measuring("fail-open-limiter", poll=0.0005), Tarpit() as tarpit:
        states = [("healthy", lab.REDIS_URL), ("refusing connections", refused_url()),
                  ("hung", tarpit.url)]
        for state, url in states:
            status_rows += statuses(state, url)
        for state, url in states:
            for client_name in CLIENTS:
                slow = state != "healthy" and client_name != "both timeouts, no retries"
                latency_rows += latency(state, url, client_name, SLOW if slow else FAST)
                print(state, "/", client_name, "done", flush=True)

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    for name, rows in (("m6_limiter_status.csv", status_rows),
                       ("m6_limiter_latency.csv", latency_rows)):
        with open(out / name, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    summary = {}
    for state in dict.fromkeys(row["limiter_redis"] for row in latency_rows):
        for client_name in CLIENTS:
            ms = sorted(row["ms"] for row in latency_rows
                        if (row["limiter_redis"], row["client"]) == (state, client_name))
            entry = {"requests": len(ms), "median_ms": statistics.median(ms),
                     "min_ms": ms[0], "max_ms": ms[-1]}
            if len(ms) >= 100:
                entry["p99_ms"] = ms[int(len(ms) * 0.99) - 1]
            summary[f"{state} / {client_name}"] = entry
    (out / "m6_limiter_summary.json").write_text(json.dumps({
        "redis_py": redis.__version__,
        "redis_py_defaults": {"socket_timeout": signature["socket_timeout"].default,
                              "socket_connect_timeout":
                                  signature["socket_connect_timeout"].default,
                              "retries": defaults._retries,
                              "backoff": type(defaults._backoff).__name__,
                              "backoff_base_s": defaults._backoff._base,
                              "backoff_cap_s": defaults._backoff._cap},
        "rounds": ROUNDS, "statuses": status_rows, "latency": summary}, indent=2))

    for row in status_rows:
        print(f"{row['limiter_redis']:22} {row['route']:22} {row['status']} "
              f"retry-after={row['retry_after'] or '-':3} {row['detail']}")
    for name, entry in summary.items():
        print(f"{name:52} n={entry['requests']:<4} median {entry['median_ms']:>9} ms  "
              f"min {entry['min_ms']:>9}  max {entry['max_ms']:>9}  p99 {entry.get('p99_ms', '')}")


if __name__ == "__main__":
    main()
