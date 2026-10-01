"""M1c: why the ON CONFLICT race answered some duplicates with 409 and others with a replay.

The claim in idempotency.py uses ON CONFLICT DO UPDATE ... WHERE (for lease takeover).
PostgreSQL locks the conflicting row for DO UPDATE even when the WHERE is false, so
duplicates may queue behind each other. This fires 99 duplicate claims at once at a key
that is already in progress, with the real CLAIM and with a plain ON CONFLICT DO NOTHING,
and times how long the last one takes to come back. 20 rounds per variant, interleaved.

Writes results/m1c_claim_pileup.csv and results/m1c_claim_pileup.json.
"""
import csv
import json
import pathlib
import statistics
import sys
import threading
import time
import uuid

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

from app import DSN  # noqa: E402
from idempotency import CLAIM, LEASE, fingerprint  # noqa: E402

DUPLICATES, ROUNDS = 99, 20
FP = fingerprint("POST", "/charges", {"amount": 1000, "currency": "usd"})
DO_NOTHING = ("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint) "
              "VALUES (%(client)s, %(key)s, %(fp)s) ON CONFLICT DO NOTHING RETURNING locked_at")
VARIANTS = {"do-update-where (the post's CLAIM)": CLAIM, "do-nothing": DO_NOTHING}


def pileup(conns: list[psycopg.Connection], sql: str, client: str) -> list[float]:
    key = str(uuid.uuid4())
    conns[0].execute("INSERT INTO idempotency_keys (client_id, idem_key, fingerprint) "
                     "VALUES (%s, %s, %s)", (client, key, FP))  # the first request's claim
    args = {"client": client, "key": key, "fp": FP, "lease": LEASE}
    barrier = threading.Barrier(len(conns) + 1)
    done = [0.0] * len(conns)

    def dup(i: int) -> None:
        barrier.wait()
        assert conns[i].execute(sql, args).fetchone() is None  # every duplicate loses
        done[i] = time.perf_counter()

    threads = [threading.Thread(target=dup, args=(i,)) for i in range(len(conns))]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    return [(d - t0) * 1000 for d in done]


def main() -> None:
    conns = [psycopg.connect(DSN, autocommit=True) for _ in range(DUPLICATES)]
    rows = []
    try:
        with measuring("idem-m1c-claim-pileup"):
            for name, sql in VARIANTS.items():  # warm-up
                pileup(conns, sql, "m1c-warmup")
            for rnd in range(1, ROUNDS + 1):
                for name, sql in VARIANTS.items():
                    ms = pileup(conns, sql, f"m1c-{rnd}")
                    rows.append({"variant": name, "round": rnd,
                                 "last_ms": round(max(ms), 2),
                                 "median_ms": round(statistics.median(ms), 2)})
                    print(rows[-1], flush=True)
        conns[0].execute("DELETE FROM idempotency_keys WHERE client_id LIKE 'm1c-%'")
    finally:
        for c in conns:
            c.close()

    with open(HERE / "results" / "m1c_claim_pileup.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = {}
    for name in VARIANTS:
        sel = [r for r in rows if r["variant"] == name]
        summary[name] = {
            "rounds": len(sel),
            "last_ms_median": statistics.median(r["last_ms"] for r in sel),
            "last_ms_min": min(r["last_ms"] for r in sel),
            "last_ms_max": max(r["last_ms"] for r in sel),
            "per_claim_median_ms": statistics.median(r["median_ms"] for r in sel),
        }
    out = {"duplicates": DUPLICATES, "rounds": ROUNDS, "results": summary}
    (HERE / "results" / "m1c_claim_pileup.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
