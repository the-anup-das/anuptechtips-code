"""Diagnostic for M1: where does a batch's time go as workers are added?

Same setup as measure_throughput.py (100,000 rows, stub publisher ~2 ms), but
each worker process times the three steps of relay.relay_batch separately:
the CLAIM statement, the publish, and the COMMIT. Also counts how many index
entries and heap pages one claim touches (EXPLAIN ANALYZE BUFFERS on a probe
claim, rolled back) half-way through a run.

    python diag_claim_cost.py
"""
from __future__ import annotations

import json
import multiprocessing as mp
import pathlib
import statistics
import sys
import time

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402
from harness import seed  # noqa: E402
from relay import CLAIM, SESSION_OPTIONS, Event, StubPublisher  # noqa: E402

ROWS = 100_000


def worker(batch: int, ready, go, results) -> None:
    pub = StubPublisher()
    claim, publish, commit = [], [], []
    with psycopg.connect(DSN, autocommit=True, options=SESSION_OPTIONS) as conn:
        ready.put(1)
        go.wait()
        while True:
            t0 = time.perf_counter()
            conn.execute("BEGIN")
            rows = conn.execute(CLAIM, (batch,)).fetchall()
            t1 = time.perf_counter()
            if not rows:
                conn.execute("COMMIT")
                break
            pub.publish(sorted((Event(*r) for r in rows), key=lambda e: e.id))
            t2 = time.perf_counter()
            conn.execute("COMMIT")
            t3 = time.perf_counter()
            claim.append(t1 - t0)
            publish.append(t2 - t1)
            commit.append(t3 - t2)
    results.put((claim, publish, commit))


def probe(batch: int) -> dict:
    with psycopg.connect(DSN, autocommit=True) as c, c.transaction(force_rollback=True):
        plan = c.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + CLAIM, (batch,)).fetchone()[0]
    top = plan[0]["Plan"]
    return {"exec_ms": round(plan[0]["Execution Time"], 2),
            "shared_hit": top.get("Shared Hit Blocks"), "shared_read": top.get("Shared Read Blocks")}


def one(workers: int, batch: int) -> dict:
    reset_schema()
    with psycopg.connect(DSN, autocommit=True) as conn:
        seed(conn, ROWS)
        conn.execute("ANALYZE outbox")
    ready, results, go = mp.Queue(), mp.Queue(), mp.Event()
    procs = [mp.Process(target=worker, args=(batch, ready, go, results)) for _ in range(workers)]
    for p in procs:
        p.start()
    for _ in procs:
        ready.get(timeout=60)
    t = time.perf_counter()
    go.set()
    time.sleep(0.6 if workers > 1 else 1.0)
    probe_mid = probe(batch)
    got = [results.get(timeout=600) for _ in procs]
    elapsed = time.perf_counter() - t
    for p in procs:
        p.join()
    claim = [x for g in got for x in g[0]]
    publish = [x for g in got for x in g[1]]
    commit = [x for g in got for x in g[2]]
    ms = lambda xs: round(statistics.median(xs) * 1000, 2)  # noqa: E731
    return {"workers": workers, "batch": batch, "elapsed_s": round(elapsed, 2),
            "claim_median_ms": ms(claim), "claim_p95_ms": round(sorted(claim)[int(len(claim) * .95)] * 1000, 2),
            "publish_median_ms": ms(publish), "commit_median_ms": ms(commit),
            "batches": len(claim), "probe_mid_run": probe_mid}


def main() -> None:
    out = []
    with measuring("outbox-diag-claim"):
        for batch in (100, 500):
            for workers in (1, 2, 4, 8):
                r = one(workers, batch)
                out.append(r)
                print(r, flush=True)
    (HERE / "results" / "diag_claim_cost.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
