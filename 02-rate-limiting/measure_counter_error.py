"""M2: the sliding window counter against the exact sliding window log.
10,000 keys, each with its own average rate (20 to 300 a minute, so 0.2x to 3x
the 100/min limit), 10 minutes of uniform (Poisson) or bursty traffic, 5 seeds.

Two questions, two frames:

A. How good is the estimate? (Cloudflare's 2017 frame.) Count every request.
   At each one, compare the counter's call ("this one would go over 100") with
   the exact count's call. Report the share of requests where they disagree and
   the average gap between the estimated and the real rate.

B. Is it a hard cap? Run the counter as the real limiter and track exactly how
   many requests it let through in the last 60 s. Report the worst 60 s of any
   key and how many keys went over 100. An exact log runs beside it for totals.

Pure Python, integer milliseconds, exact arithmetic. No timing, so no lock.
Writes results/counter_error.csv and results/counter_error_summary.json.
"""
import csv
import json
import math
import pathlib
import random
import statistics
from collections import deque
from multiprocessing import Pool

LIMIT, W = 100, 60_000
KEYS, MINUTES, SEEDS = 10_000, 10, range(5)
OUT = pathlib.Path(__file__).parent / "results"


def key_rate(rng: random.Random) -> float:
    """Requests per minute, log-uniform from 20 to 300."""
    return math.exp(rng.uniform(math.log(20), math.log(300)))


def uniform_traffic(rng, per_minute) -> list[int]:
    t, out, end = 0.0, [], MINUTES * W
    while True:
        t += rng.expovariate(per_minute / W)
        if t >= end:
            return out
        out.append(int(t))


def bursty_traffic(rng, per_minute, size=10, gap_ms=100) -> list[int]:
    """Same average rate, but in bursts of 10 requests 100 ms apart."""
    starts = uniform_traffic(rng, per_minute / size)
    return sorted(s + gap_ms * i for s in starts for i in range(size))


class Counter:
    """Sliding window counter with integer maths: the same rule as
    simulate.SlidingCounter, prev * (1 - elapsed/W) + curr + 1 <= LIMIT, times W."""

    def __init__(self):
        self.window, self.prev, self.curr = None, 0, 0

    def fits(self, t: int) -> bool:
        w, elapsed = divmod(t, W)
        if w != self.window:
            self.prev = self.curr if self.window == w - 1 else 0
            self.window, self.curr = w, 0
        return self.prev * (W - elapsed) + (self.curr + 1) * W <= LIMIT * W

    def estimate(self, t: int) -> float:
        return self.prev * (W - t % W) / W + self.curr


class Log:
    def __init__(self):
        self.times: deque[int] = deque()

    def count(self, t: int) -> int:
        while self.times and self.times[0] <= t - W:
            self.times.popleft()
        return len(self.times)


def one_key(arrivals: list[int]) -> dict:
    # A: every request counted by both
    ca, la = Counter(), Log()
    false_allow = false_limit = 0
    err_sum, err_n = 0.0, 0
    # B: the counter as the limiter, the exact count of what it let through
    cb, lb, log_only = Counter(), Log(), Log()
    counter_allowed = log_allowed = worst = 0
    for t in arrivals:
        exact = la.count(t)
        counter_fits, exact_fits = ca.fits(t), exact + 1 <= LIMIT
        false_allow += counter_fits and not exact_fits
        false_limit += exact_fits and not counter_fits
        if exact >= 50:                              # rate error near the limit
            err_sum += abs(ca.estimate(t) - exact) / exact
            err_n += 1
        ca.curr += 1
        la.times.append(t)

        in_last_60s = lb.count(t)
        if cb.fits(t):
            cb.curr += 1
            lb.times.append(t)
            counter_allowed += 1
            worst = max(worst, in_last_60s + 1)
        if log_only.count(t) < LIMIT:
            log_only.times.append(t)
            log_allowed += 1
    return {"requests": len(arrivals), "false_allow": false_allow, "false_limit": false_limit,
            "err_sum": err_sum, "err_n": err_n, "counter_allowed": counter_allowed,
            "log_allowed": log_allowed, "worst": worst}


def run_seed(job) -> dict:
    seed, kind = job
    rng = random.Random(f"{seed}-{kind}")
    make = uniform_traffic if kind == "uniform" else bursty_traffic
    tot = dict.fromkeys(("requests", "false_allow", "false_limit", "err_sum", "err_n",
                         "counter_allowed", "log_allowed"), 0)
    worst = keys_over = keys_rate_over = 0
    for _ in range(KEYS):
        rate = key_rate(rng)
        k = one_key(make(rng, rate))
        for f in tot:
            tot[f] += k[f]
        worst = max(worst, k["worst"])
        keys_over += k["worst"] > LIMIT
        keys_rate_over += rate > LIMIT
    n = tot["requests"]
    return {
        "seed": seed, "traffic": kind, "keys": KEYS, "requests": n,
        "A_wrong_pct": round(100 * (tot["false_allow"] + tot["false_limit"]) / n, 4),
        "A_false_allow_pct": round(100 * tot["false_allow"] / n, 4),
        "A_false_limit_pct": round(100 * tot["false_limit"] / n, 4),
        "A_rate_error_pct": round(100 * tot["err_sum"] / tot["err_n"], 2),
        "B_worst_60s_any_key": worst,
        "B_keys_over_100": keys_over,
        "B_keys_with_rate_over_100": keys_rate_over,
        "B_counter_allowed": tot["counter_allowed"], "B_log_allowed": tot["log_allowed"],
    }


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    jobs = [(seed, kind) for kind in ("uniform", "bursty") for seed in SEEDS]
    with Pool(10) as pool:
        rows = pool.map(run_seed, jobs)
    with open(OUT / "counter_error.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = {}
    for kind in ("uniform", "bursty"):
        rs = [r for r in rows if r["traffic"] == kind]
        summary[kind] = {f: {"median": statistics.median(r[f] for r in rs),
                             "min": min(r[f] for r in rs), "max": max(r[f] for r in rs)}
                         for f in rows[0] if f not in ("seed", "traffic", "keys")}
    (OUT / "counter_error_summary.json").write_text(json.dumps(summary, indent=2))
    for kind, s in summary.items():
        print(kind, {f: v["median"] for f, v in s.items()})
