"""M3: what the fix costs.

1. A read that lands on a closed connection has to reconnect first. Through the same
   proxy as the load test (5 ms each way), time a GET on a warm connection and a GET right
   after the pool's connection was closed, for redis-py and for the toy pool.
2. What redis-py sends when it opens a connection (handshake.py): each exchange is one more
   round trip for that read. With --old-python, also for redis-py 4.5.1 (see the README).
3. The owner check: time json.loads() alone against owned_by(), which adds the check.

Writes results/fix_cost_samples.csv and results/fix_cost.json.
"""
import argparse
import csv
import json
import os
import pathlib
import statistics
import subprocess
import sys
import time

import redis as redis_sync
import redis.asyncio as redis

from delay_proxy import DelayProxy
from owner_check import owned_by
from precise_loop import run
from toy_pool import Pool, query
from toy_server import ToyServer, profile_for

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 56379, 9
DELAY = 0.005
ROUNDS, PER_ROUND = 20, 20
CHECK_CALLS = 200_000


async def timed(make_call) -> float:
    started = time.perf_counter()
    await make_call()
    return (time.perf_counter() - started) * 1000


async def reconnect_cost() -> list[dict]:
    samples = []

    direct = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    direct.set("user:1", json.dumps(profile_for(1)))
    proxy = DelayProxy(REDIS_HOST, REDIS_PORT, DELAY)
    pool = redis.BlockingConnectionPool(host="127.0.0.1", port=await proxy.start(),
                                        db=REDIS_DB, max_connections=1, timeout=None)
    r = redis.Redis(connection_pool=pool)
    await r.get("user:1")
    for round_no in range(1, ROUNDS + 1):
        for _ in range(PER_ROUND):
            samples.append({"client": "redis-py", "round": round_no, "connection": "warm",
                            "ms": await timed(lambda: r.get("user:1"))})
            await pool.disconnect()  # what a cancelled read leaves behind
            samples.append({"client": "redis-py", "round": round_no, "connection": "closed",
                            "ms": await timed(lambda: r.get("user:1"))})
    await pool.disconnect()
    proxy.close()
    direct.delete("user:1")
    direct.close()

    server = ToyServer()
    proxy = DelayProxy("127.0.0.1", await server.start(), DELAY)
    toy = Pool("127.0.0.1", await proxy.start(), size=1)
    await query(toy, "user:1")
    for round_no in range(1, ROUNDS + 1):
        for _ in range(PER_ROUND):
            samples.append({"client": "toy pool", "round": round_no, "connection": "warm",
                            "ms": await timed(lambda: query(toy, "user:1"))})
            toy.close()
            samples.append({"client": "toy pool", "round": round_no, "connection": "closed",
                            "ms": await timed(lambda: query(toy, "user:1"))})
    toy.close()
    proxy.close()
    server.close()
    return samples


def owner_check_cost() -> dict:
    raw = json.dumps(profile_for(123)).encode()
    plain, checked = [], []
    for _ in range(ROUNDS):
        started = time.perf_counter_ns()
        for _ in range(CHECK_CALLS):
            json.loads(raw)
        plain.append((time.perf_counter_ns() - started) / CHECK_CALLS)
        started = time.perf_counter_ns()
        for _ in range(CHECK_CALLS):
            owned_by(raw, 123)
        checked.append((time.perf_counter_ns() - started) / CHECK_CALLS)
    added = [with_check - without for without, with_check in zip(plain, checked)]  # per round
    return {"calls_per_round": CHECK_CALLS, "rounds": ROUNDS, "value_bytes": len(raw),
            "json_loads_ns": round(statistics.median(plain), 1),
            "owned_by_ns": round(statistics.median(checked), 1),
            "owner_check_adds_ns": round(statistics.median(added), 1),
            "owner_check_adds_ns_min": round(min(added), 1),
            "owner_check_adds_ns_max": round(max(added), 1)}


def handshakes(pythons: list[str]) -> list[dict]:
    done = [subprocess.run([python, "handshake.py"], cwd=HERE, capture_output=True, text=True,
                           check=True) for python in pythons]
    return [json.loads(d.stdout.strip().splitlines()[-1]) for d in done]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-python", default=os.environ.get("REDIS_451_PYTHON"),
                        help="a Python that has redis==4.5.1 installed (optional)")
    args = parser.parse_args()

    with measuring("asyncio-cancellation-fix-cost", poll=0.25):
        samples = run(reconnect_cost())
        check = owner_check_cost()
    handshake = handshakes(list(filter(None, [sys.executable, args.old_python])))

    results = HERE / "results"
    results.mkdir(exist_ok=True)
    with open(results / "fix_cost_samples.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["client", "round", "connection", "ms"])
        writer.writeheader()
        writer.writerows({**s, "ms": round(s["ms"], 3)} for s in samples)

    summary = {"delay_ms_each_way": DELAY * 1000, "redis_py": redis_sync.__version__,
               "samples_per_cell": ROUNDS * PER_ROUND, "get_ms": {}, "handshake": handshake,
               "owner_check": check}
    for client in ("redis-py", "toy pool"):
        for connection in ("warm", "closed"):
            values = [s["ms"] for s in samples
                      if s["client"] == client and s["connection"] == connection]
            summary["get_ms"][f"{client}, {connection} connection"] = {
                "median": round(statistics.median(values), 2),
                "min": round(min(values), 2), "max": round(max(values), 2)}
    (results / "fix_cost.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
