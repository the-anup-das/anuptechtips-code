"""M2: what a claim costs in Postgres and in Redis, in time and in bytes.

Latency: from Python, one call at a time over localhost to the Docker containers.
- claim:  a brand-new key. Postgres runs idempotency.CLAIM; Redis runs SET NX GET EX.
- repeat: a key that is already completed, i.e. what a retry costs. Postgres runs CLAIM
          (no row back) and then the SELECT, as run_once does; Redis runs the same
          SET NX GET EX, which hands back the stored record in the same round trip.
20 rounds x 1,000 calls per store and operation, interleaved.

Bytes: 100,000 completed keys with a small JSON response each.
- Postgres: table + TOAST + both indexes, divided by the row count (after VACUUM).
- Redis: MEMORY USAGE ... SAMPLES 0 on 1,000 of the keys (the instance is shared with
  other posts' databases, so INFO memory deltas would be noise).

Writes results/m2_claim_latency_samples.csv, results/m2_claim_latency_rounds.csv and
results/m2_claim_cost.json.
"""
import csv
import json
import pathlib
import statistics
import sys
import time
import uuid

import numpy as np
import psycopg
import redis

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

from app import DSN  # noqa: E402
from idempotency import CLAIM, LEASE, fingerprint  # noqa: E402
from redis_claim import KEEP_SECONDS, LEASE_SECONDS  # noqa: E402

REDIS_URL = "redis://localhost:56379/1"
ROUNDS, CALLS, KEYS_FOR_BYTES = 20, 1000, 100_000
BODY = {"id": 1234567, "amount": 1000, "currency": "usd"}
FP = fingerprint("POST", "/charges", {"amount": 1000, "currency": "usd"})
SELECT = ("SELECT fingerprint, status, response_code, response_body FROM idempotency_keys "
          "WHERE client_id = %(client)s AND idem_key = %(key)s")


def timed(fn) -> int:
    t0 = time.perf_counter_ns()
    fn()
    return (time.perf_counter_ns() - t0) // 1000  # microseconds


def pg_round(pg: psycopg.Connection, op: str, client: str) -> list[int]:
    keys = [str(uuid.uuid4()) for _ in range(CALLS)]
    if op == "repeat":
        with pg.cursor() as cur:
            cur.executemany(
                "INSERT INTO idempotency_keys (client_id, idem_key, fingerprint, status, "
                "response_code, response_body) VALUES (%s, %s, %s, 'completed', 201, %s)",
                [(client, k, FP, json.dumps(BODY)) for k in keys])
    out = []
    for key in keys:
        args = {"client": client, "key": key, "fp": FP, "lease": LEASE}
        if op == "claim":
            out.append(timed(lambda: pg.execute(CLAIM, args).fetchone()))
        else:
            def repeat():
                assert pg.execute(CLAIM, args).fetchone() is None
                pg.execute(SELECT, args).fetchone()
            out.append(timed(repeat))
    return out


def redis_round(r: redis.Redis, op: str, client: str) -> list[int]:
    keys = [f"idem:{client}:{uuid.uuid4()}" for _ in range(CALLS)]
    if op == "repeat":
        done = json.dumps({"status": "completed", "fp": FP, "code": 201, "body": BODY})
        pipe = r.pipeline()
        for k in keys:
            pipe.set(k, done, ex=KEEP_SECONDS)
        pipe.execute()
    claim = json.dumps({"status": "in_progress", "fp": FP, "owner": uuid.uuid4().hex})
    return [timed(lambda: r.set(k, claim, nx=True, ex=LEASE_SECONDS, get=True)) for k in keys]


def pct(values: list[int], q: float) -> float:
    return float(np.percentile(values, q))


def bytes_per_key(pg: psycopg.Connection, r: redis.Redis) -> dict:
    pg.execute("DROP TABLE IF EXISTS m2_idempotency_keys")
    pg.execute("CREATE TABLE m2_idempotency_keys (LIKE idempotency_keys INCLUDING ALL)")
    pg.execute(
        "INSERT INTO m2_idempotency_keys (client_id, idem_key, fingerprint, status, "
        "response_code, response_body) "
        "SELECT 'client-' || lpad((g %% 1000)::text, 5, '0'), gen_random_uuid()::text, "
        "encode(sha256(g::text::bytea), 'hex'), 'completed', 201, "
        "jsonb_build_object('id', 1000000 + g, 'amount', 1000, 'currency', 'usd') "
        "FROM generate_series(1, %s) g", (KEYS_FOR_BYTES,))
    pg.execute("VACUUM ANALYZE m2_idempotency_keys")
    total, heap, idx = pg.execute(
        "SELECT pg_total_relation_size('m2_idempotency_keys'), "
        "pg_relation_size('m2_idempotency_keys'), "
        "pg_indexes_size('m2_idempotency_keys')").fetchone()
    pg.execute("DROP TABLE m2_idempotency_keys")

    pipe = r.pipeline()
    keys = []
    for g in range(KEYS_FOR_BYTES):
        key = f"idem:client-{g % 1000:05d}:{uuid.uuid4()}"
        rec = {"status": "completed", "fp": fingerprint("POST", "/charges", {"n": g}),
               "code": 201, "body": {"id": 1000000 + g, "amount": 1000, "currency": "usd"}}
        pipe.set(key, json.dumps(rec), ex=KEEP_SECONDS)
        keys.append(key)
        if g % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    sample = [r.memory_usage(k, samples=0) for k in keys[::KEYS_FOR_BYTES // 1000]]
    for i in range(0, len(keys), 5000):
        r.unlink(*keys[i:i + 5000])
    return {
        "keys": KEYS_FOR_BYTES,
        "postgres_total_bytes_per_key": round(total / KEYS_FOR_BYTES, 1),
        "postgres_heap_bytes_per_key": round(heap / KEYS_FOR_BYTES, 1),
        "postgres_index_bytes_per_key": round(idx / KEYS_FOR_BYTES, 1),
        "redis_memory_usage_median": statistics.median(sample),
        "redis_memory_usage_min": min(sample),
        "redis_memory_usage_max": max(sample),
        "redis_record_json_bytes": len(json.dumps(
            {"status": "completed", "fp": FP, "code": 201, "body": BODY})),
    }


def cleanup(pg: psycopg.Connection, r: redis.Redis) -> None:
    pg.execute("DELETE FROM idempotency_keys WHERE client_id LIKE 'm2-%'")
    pg.execute("DROP TABLE IF EXISTS m2_idempotency_keys")
    for pattern in ("idem:m2-*", "idem:client-*"):
        for k in r.scan_iter(pattern, count=5000):
            r.unlink(k)


def summarise(samples: list[tuple], rounds: list[dict]) -> dict:
    latency = {}
    for store in ("postgres", "redis"):
        for op in ("claim", "repeat"):
            vals = [s[3] for s in samples if s[0] == store and s[1] == op]
            per_round = [x for x in rounds if x["store"] == store and x["op"] == op]
            latency[f"{store}_{op}"] = {
                "calls": len(vals),
                "p50_us": pct(vals, 50), "p99_us": pct(vals, 99),
                "median_of_round_p50_us": statistics.median(x["p50_us"] for x in per_round),
                "median_of_round_p99_us": statistics.median(x["p99_us"] for x in per_round),
                "round_p99_min_us": min(x["p99_us"] for x in per_round),
                "round_p99_max_us": max(x["p99_us"] for x in per_round),
            }
    return latency


def main() -> None:
    samples, rounds = [], []
    with psycopg.connect(DSN, autocommit=True) as pg, redis.Redis.from_url(REDIS_URL) as r:
        pg.execute((HERE / "schema.sql").read_text())
        cleanup(pg, r)  # leftovers from an interrupted run
        try:
            with measuring("idem-m2-claim-cost"):
                for op in ("claim", "repeat"):  # warm-up, not recorded
                    pg_round(pg, op, "m2-warmup")
                    redis_round(r, op, "m2-warmup")
                for rnd in range(1, ROUNDS + 1):
                    for store, fn in (("postgres", pg_round), ("redis", redis_round)):
                        for op in ("claim", "repeat"):
                            us = fn(pg if store == "postgres" else r, op, f"m2-{rnd}")
                            samples += [(store, op, rnd, v) for v in us]
                            rounds.append({"store": store, "op": op, "round": rnd,
                                           "p50_us": pct(us, 50), "p99_us": pct(us, 99)})
                    print("round", rnd, "done", flush=True)

                # Save the timings before the (untimed) size step, so a failure there can't lose them.
                with open(HERE / "results" / "m2_claim_latency_samples.csv", "w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["store", "op", "round", "us"])
                    w.writerows(samples)
                with open(HERE / "results" / "m2_claim_latency_rounds.csv", "w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=list(rounds[0]))
                    w.writeheader()
                    w.writerows(rounds)
                size = bytes_per_key(pg, r)
        finally:
            cleanup(pg, r)

    out = {"rounds": ROUNDS, "calls_per_round": CALLS,
           "latency": summarise(samples, rounds), "bytes": size}
    (HERE / "results" / "m2_claim_cost.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
