"""M4: Redis memory per key for each algorithm at 100 requests a minute,
measured with MEMORY USAGE <key> SAMPLES 0 (every nested value counted).

Each key is driven to its full state through the real Lua script: 100 requests
for the windows and the log, a full burst for the buckets and GCRA. 200 keys
per algorithm, 5 runs, fresh keys each run. Runs under the measure lock.
Writes results/redis_memory.csv (every key) and results/redis_memory_summary.csv.
"""
import csv
import pathlib
import statistics
import sys
import uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common.measure_lock import measuring  # noqa: E402

import redis_limiters as rl  # noqa: E402

OUT = pathlib.Path(__file__).parent / "results"
KEYS_PER_RUN, RUNS = 200, 5
# (label, script, requests per key, limit per minute)
CASES = [
    ("Fixed window", "fixed_window", 100, 100),
    ("Sliding window log", "sliding_log", 100, 100),
    ("Sliding window counter", "sliding_counter", 100, 100),
    ("Token bucket", "token_bucket", 20, 100),
    ("Leaky bucket", "leaky_bucket", 21, 100),
    ("GCRA", "gcra", 20, 100),
    ("Sliding window log at 1,000/min", "sliding_log", 1000, 1000),
]


def fill(r, scripts, name, key, n, limit):
    pipe = r.pipeline(transaction=False)
    for _ in range(n):
        scripts[name](keys=[key], args=rl.args_for(name, limit=limit), client=pipe)
    pipe.execute()


def main():
    r = rl.connect()
    scripts = rl.register_all(r)
    rows = []
    for run in range(RUNS):
        for label, name, n, limit in CASES:
            keys = [f"mem:{name}:{limit}:{uuid.uuid4().hex[:8]}" for _ in range(KEYS_PER_RUN // (5 if limit > 100 else 1))]
            for key in keys:
                fill(r, scripts, name, key, n, limit)
            for key in keys:
                rows.append({"run": run, "algorithm": label, "limit_per_min": limit,
                             "type": r.type(key).decode(),
                             "encoding": r.object("encoding", key).decode(),
                             "bytes": r.memory_usage(key, samples=0)})
            r.delete(*keys)
    with open(OUT / "redis_memory.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = []
    for label, *_ in CASES:
        rs = [x for x in rows if x["algorithm"] == label]
        run_medians = [statistics.median(x["bytes"] for x in rs if x["run"] == i) for i in range(RUNS)]
        summary.append({"algorithm": label, "type": rs[0]["type"], "encoding": rs[0]["encoding"],
                        "median_bytes": statistics.median(run_medians),
                        "min_bytes": min(x["bytes"] for x in rs),
                        "max_bytes": max(x["bytes"] for x in rs), "keys": len(rs)})
    with open(OUT / "redis_memory_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    for s in summary:
        print(s)


if __name__ == "__main__":
    with measuring("rate-limiting-redis-memory"):
        main()
