"""M3: cleaning up 1,000,000 published rows: DELETE (then VACUUM) vs dropping the partition.

Setup, each run: 1,000,000 events in yesterday's partition, published by one
UPDATE (so the partial index still holds an entry for every old row version),
plus 1,000 pending events in today's partition. Autovacuum is switched off on
yesterday's partition so each step's own effect shows.

Then either DELETE the published rows and VACUUM, or drop the partition with
cleanup.drop_partition(). After each step: the relay's CLAIM (batch 100, rolled
back) timed 20 times, and the outbox's total size (heap + indexes + TOAST).

Two conditions: a quiet database, and one where a REPEATABLE READ transaction
(think: a long report) was opened before the rows were published and stays open.

    python measure_cleanup.py --runs 5
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from cleanup import drop_partition, partition_name  # noqa: E402
from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402
from relay import CLAIM  # noqa: E402

OLD_ROWS, PENDING = 1_000_000, 1_000
SIZE = "SELECT sum(pg_total_relation_size(relid)) FROM pg_partition_tree('outbox')"


def claim_ms(conn, n: int = 20) -> dict:
    times = []
    for _ in range(n):
        t = time.perf_counter()
        with conn.transaction(force_rollback=True):
            rows = conn.execute(CLAIM, (100,)).fetchall()
        times.append((time.perf_counter() - t) * 1000)
        assert len(rows) == 100
    return {"median_ms": round(statistics.median(times), 2), "max_ms": round(max(times), 2),
            "first_ms": round(times[0], 2)}


def size_mb(conn) -> float:
    return round(float(conn.execute(SIZE).fetchone()[0]) / 2**20, 1)


def setup(conn, long_tx: bool):
    reset_schema()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    part = partition_name(yesterday)
    conn.execute(f"ALTER TABLE {part} SET (autovacuum_enabled = off)")
    noon = datetime(yesterday.year, yesterday.month, yesterday.day, 12, tzinfo=timezone.utc)
    conn.execute(
        "INSERT INTO outbox (aggregatetype, aggregateid, type, payload, created_at, next_attempt_at) "
        "SELECT 'order', 'order-' || g, 'OrderPlaced', jsonb_build_object('n', g), "
        "       %s + g * interval '1 millisecond', %s + g * interval '1 millisecond' "
        "FROM generate_series(1, %s) AS g", (noon, noon, OLD_ROWS))
    conn.execute("VACUUM (ANALYZE) outbox")  # a clean start: all-visible, stats fresh
    report = None
    if long_tx:
        report = psycopg.connect(DSN)
        report.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        report.execute("SELECT count(*) FROM orders")  # takes the snapshot, holds it
    conn.execute("UPDATE outbox SET published_at = created_at + interval '50 milliseconds' "
                 "WHERE created_at < %s", (noon + timedelta(hours=12),))
    conn.execute(
        "INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
        "SELECT 'order', 'new-' || g, 'OrderPlaced', '{}' FROM generate_series(1, %s) AS g",
        (PENDING,))
    return yesterday, part, report


def one_run(long_tx: bool) -> dict:
    out: dict = {"long_transaction_open": long_tx}
    with psycopg.connect(DSN, autocommit=True) as conn:
        # DELETE path
        yesterday, part, report = setup(conn, long_tx)
        out["before"] = {**claim_ms(conn), "size_mb": size_mb(conn)}
        t = time.perf_counter()
        deleted = conn.execute("DELETE FROM outbox WHERE published_at IS NOT NULL").rowcount
        out["delete_s"] = round(time.perf_counter() - t, 2)
        out["deleted_rows"] = deleted
        out["after_delete"] = {**claim_ms(conn), "size_mb": size_mb(conn)}
        t = time.perf_counter()
        conn.execute("VACUUM outbox")
        out["vacuum_s"] = round(time.perf_counter() - t, 2)
        out["after_vacuum"] = {**claim_ms(conn), "size_mb": size_mb(conn)}
        if report:
            report.close()

        # DROP path, from an identical start
        yesterday, part, report = setup(conn, long_tx)
        out["before_drop"] = {**claim_ms(conn), "size_mb": size_mb(conn)}
        t = time.perf_counter()
        pending = drop_partition(conn, yesterday)
        out["drop_s"] = round(time.perf_counter() - t, 3)
        assert pending == 0
        out["after_drop"] = {**claim_ms(conn), "size_mb": size_mb(conn)}
        if report:
            report.close()
    return out


def main() -> None:
    global OLD_ROWS
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--rows", type=int, default=OLD_ROWS)
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    OLD_ROWS = a.rows
    runs = []
    with measuring("outbox-m3-cleanup"):
        for r in range(1, a.runs + 1):
            for long_tx in (False, True):
                row = {"run": r, **one_run(long_tx)}
                runs.append(row)
                print(json.dumps(row), flush=True)

    summary = {}
    for long_tx in (False, True):
        mine = [x for x in runs if x["long_transaction_open"] == long_tx]
        key = "long transaction open" if long_tx else "quiet database"
        med = lambda path, f: statistics.median(x[path][f] for x in mine)  # noqa: E731
        summary[key] = {
            stage: {"claim_median_ms": med(stage, "median_ms"), "size_mb": med(stage, "size_mb")}
            for stage in ("before", "after_delete", "after_vacuum", "before_drop", "after_drop")}
        summary[key]["delete_s"] = statistics.median(x["delete_s"] for x in mine)
        summary[key]["vacuum_s"] = statistics.median(x["vacuum_s"] for x in mine)
        summary[key]["drop_s"] = statistics.median(x["drop_s"] for x in mine)
        summary[key]["runs"] = len(mine)
    (HERE / "results" / f"m3_cleanup{a.tag}.json").write_text(
        json.dumps({"summary": summary, "runs": runs}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
