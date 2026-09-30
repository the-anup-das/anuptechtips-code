"""The race in numbers: 20 threads (think 20 app servers) send 25 requests each,
all at once, against a 100-request fixed window. 20 runs per limiter.
A correctness count, not a timing, so it doesn't take the measure lock.
Writes results/race.csv and results/race_summary.csv.
"""
import csv
import pathlib
import statistics
import threading
import uuid

import redis_limiters as rl

OUT = pathlib.Path(__file__).parent / "results"
THREADS, PER_THREAD, RUNS = 20, 25, 20


def count_concurrent(attempt) -> int:
    start = threading.Barrier(THREADS)
    results = []

    def worker():
        start.wait()
        results.extend(attempt() for _ in range(PER_THREAD))

    pool = [threading.Thread(target=worker) for _ in range(THREADS)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    return sum(results)


def main():
    r = rl.connect()
    fixed = rl.register_all(r)["fixed_window"]
    limiters = {
        "naive GET + INCR": lambda key: rl.naive_fixed_window(r, key, window=3600),
        "MULTI/EXEC": lambda key: rl.fixed_window_multi(r, key, window=3600),
        "Lua script": lambda key: fixed(keys=[key], args=[100, 3600])[0] == 1,
    }
    rows = []
    for run in range(RUNS):
        for name, limiter in limiters.items():
            key = f"race:{uuid.uuid4().hex}"
            rows.append({"run": run, "limiter": name, "sent": THREADS * PER_THREAD, "limit": 100,
                         "allowed": count_concurrent(lambda: limiter(key))})
            r.delete(key)
    with open(OUT / "race.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = []
    for name in limiters:
        allowed = [x["allowed"] for x in rows if x["limiter"] == name]
        summary.append({"limiter": name, "runs": RUNS, "median_allowed": statistics.median(allowed),
                        "min_allowed": min(allowed), "max_allowed": max(allowed)})
    with open(OUT / "race_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    for s in summary:
        print(s)


if __name__ == "__main__":
    main()
