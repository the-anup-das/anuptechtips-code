"""M1c: where does the burst cost in measure_burst.py come from? No gateway here.

One httpx.AsyncClient sends 64 requests at the same instant to the mock, which answers at
once. First through an empty connection pool (64 new connections), then five more times
through the same pool, which now holds 64 idle keep-alive connections. The time is how
long the whole burst takes, start to last response.

A final burst runs under cProfile to count calls: httpcore 1.0.9 walks every pooled
connection each time a request joins or leaves the pool, and asks each idle one whether
its socket has become readable. The profiled burst itself is slower, so only its call
counts are kept.

    python measure_pool_burst.py         # 10 clients: 10 cold bursts, 50 warm ones

Writes results/m1c_pool_burst.json.
"""
import argparse
import asyncio
import cProfile
import json
import pathlib
import platform
import pstats
import statistics
import sys
import time

import httpcore
import httpx

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import harness  # noqa: E402

BURST = 64
BODY = {"model": "alpha-large", "max_tokens": 20,
        "messages": [{"role": "user", "content": "Summarize this refund request in one line"}]}
HEADERS = {"Authorization": "Bearer sk-mock-alpha"}
LIMITS = httpx.Limits(max_connections=2000, max_keepalive_connections=2000)
COUNTED = ("_assign_requests_to_connections", "is_socket_readable")


async def burst(client: httpx.AsyncClient, url: str) -> float:
    """Milliseconds for BURST simultaneous requests, start to last response."""
    started = time.perf_counter()
    responses = await asyncio.gather(*(client.post(url, json=BODY, headers=HEADERS)
                                       for _ in range(BURST)))
    assert all(resp.status_code == 200 for resp in responses)
    return round((time.perf_counter() - started) * 1000, 1)


async def main(url: str, clients: int) -> dict:
    cold, warm = [], []
    for _ in range(clients):
        async with httpx.AsyncClient(timeout=30, limits=LIMITS) as client:
            cold.append(await burst(client, url))      # an empty pool
            for _ in range(5):
                warm.append(await burst(client, url))  # 64 idle keep-alive connections
    async with httpx.AsyncClient(timeout=30, limits=LIMITS) as client:
        await burst(client, url)
        profile = cProfile.Profile()
        profile.enable()
        await burst(client, url)
        profile.disable()
    calls = {}
    for (_, _, name), (_, n_calls, _, _, _) in pstats.Stats(profile).stats.items():
        if name in COUNTED:
            calls[name] = calls.get(name, 0) + n_calls
    return {"cold_ms": cold, "warm_ms": warm, "calls_in_one_warm_burst": calls}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=10)
    args = ap.parse_args()

    with harness.spawn("mock_upstream:app", log_name="m1c_mock") as mock:
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        with measuring("llmgw-m1c-pool-burst", poll=0.002):
            data = asyncio.run(main(f"{mock}/alpha/v1/chat/completions", args.clients))

    summary = {
        "started": started,
        "setup": {"burst": BURST, "clients": args.clients, "warm_bursts_per_client": 5,
                  "python": platform.python_version(), "httpx": httpx.__version__,
                  "httpcore": httpcore.__version__,
                  "mock": "mock_upstream:app, 1 uvicorn process, answers at once"},
        "results": {
            name: {"median": statistics.median(data[key]), "min": min(data[key]),
                   "max": max(data[key]), "bursts": len(data[key])}
            for name, key in (("empty pool", "cold_ms"), ("64 idle connections", "warm_ms"))
        },
        "calls_in_one_warm_burst": data["calls_in_one_warm_burst"],
        "bursts_ms": {"empty pool": data["cold_ms"], "64 idle connections": data["warm_ms"]},
    }
    (HERE / "results" / "m1c_pool_burst.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: summary[k] for k in ("results", "calls_in_one_warm_burst")}, indent=2))
