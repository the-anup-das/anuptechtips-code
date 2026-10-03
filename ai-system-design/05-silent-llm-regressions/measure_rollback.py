"""M6: how long a rollback takes to reach every worker.

Eight worker threads keep asking which version to serve, all on the candidate. The main
thread calls rollout.rollback() and each worker notes when it first gets the stable version.
Run once with a Redis read per request, and once with the flag cached for a second (the
workers' caches refresh an eighth of a second apart).

A flag flip on a toy says nothing about rolling back a real model fleet; see the post.

    python measure_rollback.py [--runs 20]
"""
import argparse
import csv
import json
import pathlib
import statistics
import sys
import threading
import time

import redis

import harness
import rollout
from flag_cache import CachedVersions

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))   # the series folder
from common.measure_lock import measuring  # noqa: E402

WORKERS = 8
CACHE_TTL = 1.0
RESULTS = harness.HERE / "results"


def one_run(r: redis.Redis, cached: bool) -> dict:
    rollout.start(r, stable="model-1", candidate="model-2", percent=100)
    switched: list[float] = [0.0] * WORKERS      # when each worker first got the stable version
    late: list[int] = [0] * WORKERS              # candidate replies read after rollback() returned
    served: list[int] = [0] * WORKERS
    returned: list[float] = []                   # set once rollback() has returned
    ready = threading.Barrier(WORKERS + 1)

    def worker(i: int) -> None:
        client = redis.Redis.from_url(rollout.REDIS_URL)
        reader = CachedVersions(client, CACHE_TTL) if cached else None
        user = f"user-{i}"
        get = (lambda: reader.version_for(user)) if cached else \
            (lambda: rollout.version_for(client, user))
        if cached:
            time.sleep(i * CACHE_TTL / WORKERS)   # processes don't refresh their caches in step
        assert get() == "model-2"
        ready.wait()
        while True:
            asked = time.perf_counter()
            version = get()
            served[i] += 1
            if version == "model-1":
                switched[i] = time.perf_counter()
                break
            if returned and asked > returned[0]:
                late[i] += 1
            if cached:
                time.sleep(0.001)    # a cached read costs nothing: don't spin the CPU
        client.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
    for t in threads:
        t.start()
    ready.wait()
    time.sleep(0.25)                             # every worker is busy serving the candidate
    called = time.perf_counter()
    rollout.rollback(r)
    returned.append(time.perf_counter())
    for t in threads:
        t.join()
    return {
        "mode": "cached 1 s" if cached else "read per request",
        "rollback_call_ms": (returned[0] - called) * 1000,
        "all_workers_switched_ms": (max(switched) - called) * 1000,
        "first_worker_switched_ms": (min(switched) - called) * 1000,
        "candidate_replies_after_return": sum(late),
        "requests_served": sum(served),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    runs = parser.parse_args().runs
    r = redis.Redis.from_url(rollout.REDIS_URL)
    rows = []
    with measuring("silent-llm-regressions-rollback"):
        for cached in (False, True):
            one_run(r, cached)                   # warm-up, not recorded
            for run in range(runs):
                rows.append({"run": run, **one_run(r, cached)})
    r.delete(rollout.KEY)

    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / "m6_rollback_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {"runs": runs, "workers": WORKERS, "cache_ttl_seconds": CACHE_TTL, "modes": {}}
    for mode in ("read per request", "cached 1 s"):
        cell = [row for row in rows if row["mode"] == mode]
        summary["modes"][mode] = {
            key: {"median": round(statistics.median(row[key] for row in cell), 3),
                  "min": round(min(row[key] for row in cell), 3),
                  "max": round(max(row[key] for row in cell), 3)}
            for key in rows[0] if key not in ("run", "mode")}
    summary["setup"] = harness.MACHINE + "; Python 3.12, redis-py 8.1.0, Redis 8.10.2"
    (RESULTS / "m6_rollback_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
