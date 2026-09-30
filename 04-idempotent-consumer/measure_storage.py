"""M4: bytes per processed_messages row, for text vs uuid message IDs.

    python measure_storage.py --runs 5

Each run fills a fresh copy of the inbox table with 1,000,000 rows, VACUUMs it, and
reads the heap, index and total sizes. Random UUIDv4 keys land all over the primary-key
index; Postgres 18's uuidv7() keys arrive in order. Sizes aren't timings, but the bulk
inserts are heavy, so the runs hold the measure lock to stay out of other benchmarks.
"""
import argparse
import json
import statistics
import sys

import psycopg

import harness as h

sys.path.insert(0, str(h.HERE.parent))
from common.measure_lock import measuring  # noqa: E402

ROWS = 1_000_000
VARIANTS = {
    "text, UUIDv4 string": ("text", "gen_random_uuid()::text"),
    "uuid, v4 (random)": ("uuid", "gen_random_uuid()"),
    "uuid, v7 (time-ordered)": ("uuid", "uuidv7()"),
}


def measure(conn, typ: str, expr: str) -> dict:
    conn.execute("DROP TABLE IF EXISTS m4_inbox")
    conn.execute(f"""CREATE TABLE m4_inbox (
        consumer     text        NOT NULL,
        message_id   {typ}       NOT NULL,
        processed_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (consumer, message_id))""")
    conn.execute(f"INSERT INTO m4_inbox (consumer, message_id) "
                 f"SELECT 'wallet-credits', {expr} FROM generate_series(1, {ROWS})")
    conn.execute("VACUUM (ANALYZE) m4_inbox")
    heap, index, total = conn.execute(
        "SELECT pg_relation_size('m4_inbox'), pg_indexes_size('m4_inbox'), "
        "pg_total_relation_size('m4_inbox')").fetchone()
    conn.execute("DROP TABLE m4_inbox")
    return {"heap_bytes": heap, "index_bytes": index, "total_bytes": total,
            "heap_per_row": heap / ROWS, "index_per_row": index / ROWS,
            "total_per_row": total / ROWS}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()
    dsn = h.setup_schema("m4")
    raw = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        pg = conn.execute("SHOW server_version").fetchone()[0]
        for run in range(1, args.runs + 1):
            for name, (typ, expr) in VARIANTS.items():
                with measuring(f"consumer-storage-{run}"):
                    r = measure(conn, typ, expr)
                r.update(variant=name, run=run)
                raw.append(r)
                print(json.dumps(r), flush=True)
    summary = []
    for name in VARIANTS:
        rs = [r for r in raw if r["variant"] == name]
        summary.append({
            "variant": name,
            "total_per_row_median": round(statistics.median(r["total_per_row"] for r in rs), 1),
            "heap_per_row_median": round(statistics.median(r["heap_per_row"] for r in rs), 1),
            "index_per_row_median": round(statistics.median(r["index_per_row"] for r in rs), 1),
            "total_per_row_min": round(min(r["total_per_row"] for r in rs), 1),
            "total_per_row_max": round(max(r["total_per_row"] for r in rs), 1),
            "runs": len(rs),
        })
    out = {"rows": ROWS, "postgres": pg, "summary": summary, "raw": raw}
    (h.HERE / "results" / "m4_storage.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(summary, indent=1))
    h.drop_schema("m4")


if __name__ == "__main__":
    main()
