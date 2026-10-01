"""M1 + M3: the same 100/min limit and the same traffic through all six algorithms.

Pure-Python simulation, so no timing and no measure lock. Deterministic patterns
run once (a rerun gives the same numbers); the random bursty pattern runs 20 seeds.
Writes results/edge_burst.csv, results/random_bursts.csv,
results/gcra_vs_token_bucket.json and results/counter_trace.csv.
"""
import csv
import json
import pathlib
import statistics
from collections import deque

import simulate as s

OUT = pathlib.Path(__file__).parent / "results"


def patterns_table() -> list[dict]:
    rows = []
    for pattern, make in s.PATTERNS.items():
        arrivals = make()
        for name, limiter in s.all_limiters().items():
            if isinstance(limiter, s.LeakyBucketQueue):
                offers = [(t, limiter.offer(t)) for t in arrivals]
                served = [r for _, r in offers if r is not None]
                max_delay = max(float(r - t) for t, r in offers if r is not None)
            else:
                served, max_delay = s.run(limiter, arrivals), 0.0
            rows.append({
                "pattern": pattern, "algorithm": name, "sent": len(arrivals),
                "served": len(served),
                "max_in_1s": s.max_in_span(served, 1000),
                "max_in_60s": s.max_in_span(served, s.WINDOW_MS),
                "max_delay_ms": round(max_delay),
            })
    return rows


def random_bursts_table(seeds=range(20)) -> list[dict]:
    per_algo: dict[str, list[int]] = {}
    for seed in seeds:
        arrivals = s.random_bursts(seed)
        for name, limiter in s.all_limiters().items():
            per_algo.setdefault(name, []).append(s.max_in_span(s.run(limiter, arrivals), s.WINDOW_MS))
    return [{"algorithm": name, "seeds": len(v), "median_max_in_60s": statistics.median(v),
             "min": min(v), "max": max(v), "all": " ".join(map(str, v))}
            for name, v in per_algo.items()]


def gcra_vs_token_bucket(seeds=range(20)) -> dict:
    """M3: does GCRA make exactly the token bucket's decisions?"""
    compared = mismatches = 0
    traffic = [make() for make in s.PATTERNS.values()] + [s.random_bursts(i) for i in seeds]
    for arrivals in traffic:
        for burst in (1, 5, 20, 100):
            gcra, bucket = s.GCRA(burst=burst), s.TokenBucket(capacity=burst)
            for t in arrivals:
                compared += 1
                mismatches += gcra.allow(t) != bucket.allow(t)
    return {"decisions_compared": compared, "mismatches": mismatches,
            "bursts": [1, 5, 20, 100], "traffic_patterns": len(traffic)}


def counter_trace() -> list[dict]:
    """The counter's own estimate vs the exact number it let through in the
    last 60 s, at every request of the main pattern."""
    counter, admitted, rows = s.SlidingCounter(), deque(), []
    for t in s.edge_burst_then_steady():
        while admitted and admitted[0] <= t - s.WINDOW_MS:
            admitted.popleft()
        estimate = counter.estimate(t)
        allowed = counter.allow(t)
        if allowed:
            admitted.append(t)
        rows.append({"t_ms": t, "estimate": round(float(estimate), 3),
                     "exact_last_60s": len(admitted), "allowed": int(allowed)})
    return rows


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    table = patterns_table()
    write_csv(OUT / "edge_burst.csv", table)
    write_csv(OUT / "random_bursts.csv", random_bursts_table())
    write_csv(OUT / "counter_trace.csv", counter_trace())
    m3 = gcra_vs_token_bucket()
    (OUT / "gcra_vs_token_bucket.json").write_text(json.dumps(m3, indent=2))
    for row in table:
        if row["pattern"] == "edge burst, then 3x for 4 min":
            print(f"{row['algorithm']:32} 1 s: {row['max_in_1s']:>4}   60 s: {row['max_in_60s']:>4}"
                  f"   max delay {row['max_delay_ms']} ms")
    print("GCRA vs token bucket:", m3)
