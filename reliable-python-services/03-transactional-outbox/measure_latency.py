"""M2a: commit-to-publish latency: 1 s polling vs 100 ms polling vs LISTEN/NOTIFY.

Four writer threads commit 50 orders/s each (200 commits/s in total), one
event per commit. One relay thread (batch 100) hands events to a stub
publisher that notes when each one arrives. Latency = that moment minus the
moment the writer called COMMIT, both from time.perf_counter() in one process.
The first second of every run is warm-up and isn't counted.

    python measure_latency.py --runs 5
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import statistics
import sys
import threading
import time

import numpy as np
import psycopg
from psycopg.types.json import Jsonb

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402
from relay import StubPublisher, run  # noqa: E402
from wakeup import run_with_notify  # noqa: E402

WRITERS, RATE_EACH, DURATION, WARMUP, BATCH = 4, 50, 10.0, 1.0, 100
MODES = ["poll 1 s", "poll 100 ms", "LISTEN/NOTIFY"]

ORDER = "INSERT INTO orders (customer_id, total_cents) VALUES (%s, %s) RETURNING id"
EVENT = ("INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
         "VALUES ('order', %s, 'OrderPlaced', %s) RETURNING id")


class TimingPublisher(StubPublisher):
    def __init__(self):
        super().__init__(delay=0.002)
        self.at: dict = {}

    def publish(self, events):
        now = time.perf_counter()
        for e in events:
            self.at.setdefault(e.id, now)
        return super().publish(events)


def writer(i: int, start: float, stop: float, committed: dict) -> None:
    with psycopg.connect(DSN) as conn:
        next_t = start + i * (1 / RATE_EACH / WRITERS)  # stagger the four writers
        while next_t < stop:
            delay = next_t - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            order_id = conn.execute(ORDER, (f"cust-{i}", 1999)).fetchone()[0]
            event_id = conn.execute(EVENT, (str(order_id), Jsonb({"order_id": str(order_id)}))).fetchone()[0]
            t = time.perf_counter()
            conn.commit()
            committed[event_id] = t
            next_t += 1 / RATE_EACH


def one_run(mode: str) -> tuple[list[float], dict]:
    reset_schema(notify=(mode == "LISTEN/NOTIFY"))
    pub, stop_relay = TimingPublisher(), threading.Event()
    if mode == "LISTEN/NOTIFY":
        target, kw = run_with_notify, dict(batch_size=BATCH, fallback_poll=1.0)
    else:
        target, kw = run, dict(batch_size=BATCH, poll_interval=1.0 if mode == "poll 1 s" else 0.1)
    relay = threading.Thread(target=target, args=(DSN, pub), kwargs={**kw, "stop": stop_relay.is_set},
                             daemon=True)
    relay.start()
    time.sleep(0.5)

    committed: dict = {}
    start = time.perf_counter() + 0.2
    stop = start + DURATION
    writers = [threading.Thread(target=writer, args=(i, start, stop, committed)) for i in range(WRITERS)]
    for w in writers:
        w.start()
    for w in writers:
        w.join()
    deadline = time.perf_counter() + 5
    while time.perf_counter() < deadline and not all(k in pub.at for k in committed):
        time.sleep(0.05)
    stop_relay.set()
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("NOTIFY outbox")  # wake a LISTEN relay so it sees the stop flag
    relay.join(5)

    counted = {k: t for k, t in committed.items() if t >= start + WARMUP}
    lat = [(pub.at[k] - t) * 1000 for k, t in counted.items() if k in pub.at]
    stats = {"mode": mode, "events": len(counted), "missing": len(counted) - len(lat),
             "commits_per_s": round(len(committed) / DURATION, 1),
             "p50_ms": round(float(np.percentile(lat, 50)), 2),
             "p95_ms": round(float(np.percentile(lat, 95)), 2),
             "p99_ms": round(float(np.percentile(lat, 99)), 2),
             "max_ms": round(max(lat), 2)}
    return lat, stats


def main() -> None:
    global DURATION
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--duration", type=float, default=DURATION)
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    DURATION = a.duration
    raw, runs = [], []
    with measuring("outbox-m2-latency"):
        for r in range(1, a.runs + 1):
            for mode in MODES:
                lat, stats = one_run(mode)
                stats["run"] = r
                runs.append(stats)
                raw += [(mode, r, round(x, 3)) for x in lat]
                print(stats, flush=True)
    with (HERE / "results" / f"m2_latency_raw{a.tag}.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mode", "run", "latency_ms"])
        w.writerows(raw)
    summary = {}
    for mode in MODES:
        mine = [s for s in runs if s["mode"] == mode]
        pooled = [x for m, _, x in raw if m == mode]
        summary[mode] = {
            "median_of_run_p50_ms": statistics.median(s["p50_ms"] for s in mine),
            "median_of_run_p99_ms": statistics.median(s["p99_ms"] for s in mine),
            "min_run_p99_ms": min(s["p99_ms"] for s in mine),
            "max_run_p99_ms": max(s["p99_ms"] for s in mine),
            "pooled_p50_ms": round(float(np.percentile(pooled, 50)), 2),
            "pooled_p99_ms": round(float(np.percentile(pooled, 99)), 2),
            "events": len(pooled), "missing": sum(s["missing"] for s in mine), "runs": len(mine)}
    (HERE / "results" / f"m2_latency_summary{a.tag}.json").write_text(
        json.dumps({"summary": summary, "runs": runs}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
