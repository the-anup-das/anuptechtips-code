"""M1a: relay throughput with 1, 2, 4 and 8 SKIP LOCKED worker processes.

Each run: fresh schema, 100,000 pending rows, N relay_worker.py processes with
the stub publisher (about 2 ms per batch), all started together. Throughput is
rows / (last commit - first claim). Every run also checks 0 missing and 0
duplicates from the stub's id logs.

    python measure_throughput.py            # 5 runs per setting
    python measure_throughput.py --runs 1   # quick look
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import shutil
import statistics
import sys
import tempfile

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402
from harness import count_sent, finish, go, seed, spawn  # noqa: E402

ROWS = 100_000
WORKERS = [1, 2, 4, 8]
BATCHES = [10, 100, 500]


def one_run(workers: int, batch: int, tmp: pathlib.Path) -> dict:
    reset_schema()
    with psycopg.connect(DSN, autocommit=True) as conn:
        seed(conn, ROWS)
        conn.execute("ANALYZE outbox")
        logs = [tmp / f"w{workers}-b{batch}-{i}.log" for i in range(workers)]
        for log in logs:
            log.unlink(missing_ok=True)
        procs = [spawn(batch=batch, log=log) for log in logs]
        go(procs)
        results = [finish(p, timeout=900) for p in procs]
        left = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
        all_ids = {str(r[0]) for r in conn.execute("SELECT id FROM outbox")}
    sent = count_sent(logs)
    first = min(r[1] for r in results)
    last = max(r[2] for r in results)
    return {
        "workers": workers, "batch": batch, "rows": sum(r[0] for r in results),
        "seconds": round(last - first, 4), "msgs_per_s": round(ROWS / (last - first), 1),
        "missing": len(all_ids - set(sent)), "duplicates": sum(c - 1 for c in sent.values()),
        "left_unpublished": left,
        "per_worker_rows": "/".join(str(r[0]) for r in results),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    a = ap.parse_args()
    out = HERE / "results" / "m1_throughput.csv"
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="outbox-m1-"))
    rows = []
    try:
        with measuring("outbox-m1-throughput"):
            for run in range(1, a.runs + 1):  # interleave settings, so drift hits them all
                for batch in BATCHES:
                    for workers in WORKERS:
                        r = {"run": run, **one_run(workers, batch, tmp)}
                        rows.append(r)
                        print(r, flush=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if rows:
            with out.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)

    summary = {}
    for batch in BATCHES:
        for workers in WORKERS:
            vals = [r["msgs_per_s"] for r in rows if r["batch"] == batch and r["workers"] == workers]
            summary[f"batch={batch} workers={workers}"] = {
                "median_msgs_per_s": statistics.median(vals), "min": min(vals), "max": max(vals),
                "runs": len(vals)}
    summary["missing_total"] = sum(r["missing"] for r in rows)
    summary["duplicates_total"] = sum(r["duplicates"] for r in rows)
    (HERE / "results" / "m1_throughput_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
