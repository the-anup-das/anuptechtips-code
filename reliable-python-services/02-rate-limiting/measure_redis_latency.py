"""M5: latency per rate-limit decision against the Docker Redis, one client,
calls in sequence. Variants: the racy read-then-write, a MULTI/EXEC fixed
window, and each of the six Lua limiters called by EVALSHA and by FCALL.

20 runs per variant (variant order rotated each run), 2,000 timed calls per run
after 200 warm-up calls, spread over 1,000 keys. Reports the median across runs
of each run's p50 and p99. Runs under the measure lock.
Writes results/redis_latency_runs.csv and results/redis_latency_summary.csv.
"""
import csv
import pathlib
import statistics
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common.measure_lock import measuring  # noqa: E402

import redis_limiters as rl  # noqa: E402

OUT = pathlib.Path(__file__).parent / "results"
RUNS, CALLS, WARMUP, KEYS = 20, 2000, 200, 1000


def variants(r):
    scripts = rl.register_all(r)
    rl.load_functions(r)
    v = {
        "naive GET + INCR (racy)": lambda k: rl.naive_fixed_window(r, k),
        "MULTI/EXEC fixed window": lambda k: rl.fixed_window_multi(r, k),
    }
    for name in rl.ALGORITHMS:
        v[f"EVALSHA {name}"] = (lambda n: lambda k: scripts[n](keys=[k], args=rl.args_for(n)))(name)
        v[f"FCALL {name}"] = (lambda n: lambda k: rl.fcall(r, n, k, rl.args_for(n)))(name)
    return v


def percentile(sorted_us, p):
    return sorted_us[min(len(sorted_us) - 1, int(p / 100 * len(sorted_us)))]


def main():
    r = rl.connect()
    vs = variants(r)
    names = list(vs)
    runs = []
    for run in range(RUNS):
        order = names[run % len(names):] + names[:run % len(names)]
        for name in order:
            call = vs[name]
            keys = [f"lat:{run}:{names.index(name)}:{i}" for i in range(KEYS)]
            for i in range(WARMUP):
                call(keys[i % KEYS])
            lat = []
            for i in range(CALLS):
                k = keys[(WARMUP + i) % KEYS]
                t0 = time.perf_counter_ns()
                call(k)
                lat.append((time.perf_counter_ns() - t0) / 1000)
            lat.sort()
            runs.append({"run": run, "variant": name, "p50_us": round(percentile(lat, 50), 1),
                         "p99_us": round(percentile(lat, 99), 1), "mean_us": round(statistics.mean(lat), 1)})
            r.delete(*keys)
    with open(OUT / "redis_latency_runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(runs[0]))
        w.writeheader()
        w.writerows(runs)
    summary = []
    for name in names:
        rs = [x for x in runs if x["variant"] == name]
        summary.append({"variant": name, "runs": len(rs),
                        "median_p50_us": round(statistics.median(x["p50_us"] for x in rs), 1),
                        "min_p50_us": min(x["p50_us"] for x in rs),
                        "max_p50_us": max(x["p50_us"] for x in rs),
                        "median_p99_us": round(statistics.median(x["p99_us"] for x in rs), 1),
                        "max_p99_us": max(x["p99_us"] for x in rs)})
    with open(OUT / "redis_latency_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    for s in summary:
        print(f"{s['variant']:28} p50 {s['median_p50_us']:7.1f} us (min {s['min_p50_us']}, max {s['max_p50_us']})"
              f"   p99 {s['median_p99_us']:7.1f} us")


if __name__ == "__main__":
    with measuring("rate-limiting-redis-latency"):
        main()
