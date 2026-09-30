"""M2b: what the NOTIFY trigger costs the writers.

N writer processes commit as fast as they can for 10 s, each transaction an
INSERT into orders plus an INSERT into outbox, with and without notify.sql's
trigger. A LISTEN connection drains notifications in both modes. Up to
PostgreSQL 18, a transaction that sent NOTIFY takes a global lock at commit,
so notifying commits queue behind each other.

    python measure_notify_overhead.py --runs 5
"""
from __future__ import annotations

import argparse
import csv
import json
import multiprocessing as mp
import pathlib
import statistics
import sys
import threading
import time

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402

WRITER_COUNTS = [1, 8, 32]
DURATION = 10.0

ORDER = "INSERT INTO orders (customer_id, total_cents) VALUES ('cust-1', 1999) RETURNING id"
EVENT = ("INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
         "VALUES ('order', %s, 'OrderPlaced', '{}')")


def writer(ready, go, stop_at, results) -> None:
    with psycopg.connect(DSN) as conn:
        ready.put(1)
        go.wait()
        n = 0
        while time.perf_counter() < stop_at.value:
            order_id = conn.execute(ORDER).fetchone()[0]
            conn.execute(EVENT, (str(order_id),))
            conn.commit()
            n += 1
        results.put(n)


def drain(stop: threading.Event, counter: list) -> None:
    with psycopg.connect(DSN, autocommit=True) as listener:
        listener.execute("LISTEN outbox")
        while not stop.is_set():
            counter[0] += sum(1 for _ in listener.notifies(timeout=0.2))


def one_run(writers: int, notify: bool) -> dict:
    reset_schema(notify=notify)
    ready, results = mp.Queue(), mp.Queue()
    go, stop_at = mp.Event(), mp.Value("d", 0.0)
    procs = [mp.Process(target=writer, args=(ready, go, stop_at, results)) for _ in range(writers)]
    for p in procs:
        p.start()
    for _ in procs:
        ready.get(timeout=60)
    stop_drain, notes = threading.Event(), [0]
    t = threading.Thread(target=drain, args=(stop_drain, notes))
    t.start()
    time.sleep(0.3)
    stop_at.value = time.perf_counter() + DURATION
    go.set()
    commits = sum(results.get(timeout=120) for _ in procs)
    for p in procs:
        p.join()
    stop_drain.set()
    t.join()
    return {"writers": writers, "notify": notify, "commits": commits,
            "commits_per_s": round(commits / DURATION, 1), "notifications": notes[0]}


def main() -> None:
    global DURATION
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--duration", type=float, default=DURATION)
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    DURATION = a.duration
    rows = []
    with measuring("outbox-m2-notify-overhead"):
        for r in range(1, a.runs + 1):
            for writers in WRITER_COUNTS:
                for notify in (False, True):
                    row = {"run": r, **one_run(writers, notify)}
                    rows.append(row)
                    print(row, flush=True)
    with (HERE / "results" / f"m2_notify_overhead{a.tag}.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = {}
    for writers in WRITER_COUNTS:
        off = [x["commits_per_s"] for x in rows if x["writers"] == writers and not x["notify"]]
        on = [x["commits_per_s"] for x in rows if x["writers"] == writers and x["notify"]]
        summary[f"{writers} writers"] = {
            "without_notify_median": statistics.median(off), "with_notify_median": statistics.median(on),
            "without_min_max": [min(off), max(off)], "with_min_max": [min(on), max(on)],
            "change_pct": round(100 * (statistics.median(on) / statistics.median(off) - 1), 1)}
    (HERE / "results" / f"m2_notify_overhead_summary{a.tag}.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
