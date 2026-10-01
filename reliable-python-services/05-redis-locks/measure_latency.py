"""Measurement 2, single-instance part only: acquire + release round trip, uncontended.

Only one Redis runs in this environment, so Redlock across 3 or 5 instances is not
measured here. Loopback through Docker Desktop, so these are best-case numbers.
Timing, so it runs under the measure lock.

    python measure_latency.py   -> results/latency_samples.csv, results/latency_summary.csv
"""
import csv
import os
import pathlib
import statistics
import sys
import time

import psycopg
import redis

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402
from advisory import try_lock  # noqa: E402
from fencing import FencedLock  # noqa: E402
from simple_lock import acquire, release  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
PG_DSN = os.environ.get("PG_DSN", "postgresql://patterns:patterns@localhost:55432/locks")
RUNS, CYCLES, WARMUP = 5, 2000, 200


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL)
    db = psycopg.connect(PG_DSN)

    def redis_py_lock():
        lock = r.lock("m2:redispy", timeout=5)
        lock.acquire(blocking=False)
        lock.release()

    def set_nx_delex():
        token = acquire(r, "m2:simple", ttl_ms=5000, wait_s=0)
        release(r, "m2:simple", token)

    def fenced_lock():
        lock = FencedLock(r, "{m2:fenced}", timeout=5)
        lock.acquire(blocking=False)
        lock.release()

    def advisory_xact():
        with db.transaction():
            try_lock(db, "m2:advisory")

    cases = {"redis-py Lock": redis_py_lock, "SET NX PX + DELEX": set_nx_delex,
             "FencedLock (Lua + INCR)": fenced_lock, "pg_try_advisory_xact_lock": advisory_xact}
    samples = []
    with measuring("redis-locks-latency"):
        for run in range(1, RUNS + 1):
            for name, fn in cases.items():
                for _ in range(WARMUP):
                    fn()
                for _ in range(CYCLES):
                    t = time.perf_counter()
                    fn()
                    samples.append((name, run, (time.perf_counter() - t) * 1000))
    db.close()
    with open(HERE / "results" / "latency_samples.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["case", "run", "ms"])
        w.writerows((c, k, round(ms, 4)) for c, k, ms in samples)
    summary = []
    for name in cases:
        ms = sorted(s[2] for s in samples if s[0] == name)
        run_p50 = [statistics.median(s[2] for s in samples if s[0] == name and s[1] == k)
                   for k in range(1, RUNS + 1)]
        summary.append(dict(case=name, samples=len(ms), p50_ms=round(statistics.median(ms), 3),
                            p99_ms=round(ms[int(0.99 * (len(ms) - 1))], 3),
                            run_p50_min_ms=round(min(run_p50), 3), run_p50_max_ms=round(max(run_p50), 3)))
    with open(HERE / "results" / "latency_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, summary[0].keys())
        w.writeheader()
        w.writerows(summary)
    for s in summary:
        print(s)


if __name__ == "__main__":
    main()
