"""M3: what the inbox insert costs. 100,000 events, one consumer, no kills, no simulated work.

    python measure_throughput.py --runs 5

Each run pre-produces the events (not timed), starts one consumer process and times it
from its first side effect to its last, using the ledger's clock_timestamp() column.
The gap between consecutive side effects is the per-message cycle: poll, transaction,
offset commit. Every run holds the shared measure lock.
"""
import argparse
import json
import statistics
import sys
import time
import uuid

import psycopg

import harness as h

sys.path.insert(0, str(h.HERE.parent))
from common.measure_lock import measuring  # noqa: E402

N_EVENTS = 100_000
VARIANTS = ["b", "c", "a1", "a2"]


def one_run(variant: str, run: int) -> dict:
    schema = f"m3_{variant}_{run}"
    dsn = h.setup_schema(schema)
    topic = f"consumer.m3.{variant}.{run}.{uuid.uuid4().hex[:6]}"
    h.create_topic(topic, 6)
    h.produce_events(topic, N_EVENTS)
    log = h.HERE / "results" / "logs" / f"m3_{variant}_{run}.log"
    log.unlink(missing_ok=True)
    with measuring(f"consumer-throughput-{variant}-{run}"):
        proc = h.start_worker(variant, topic, topic, dsn, log)
        t0 = time.time()
        while h.ledger_max_id(dsn) < N_EVENTS:  # cheap check; no kills means no duplicates
            time.sleep(1)
            if time.time() - t0 > 1800:
                raise RuntimeError("too slow")
        proc.kill()
        proc.wait()
    with psycopg.connect(dsn) as c:
        ts = [r[0] for r in c.execute("SELECT extract(epoch FROM created_at)::float8 FROM ledger ORDER BY id")]
    gaps_ms = sorted((b - a) * 1000 for a, b in zip(ts, ts[1:]))
    span = ts[-1] - ts[0]
    result = {
        "variant": variant, "run": run, "events": N_EVENTS, "span_s": round(span, 2),
        "msgs_per_s": round((len(ts) - 1) / span, 1),
        "gap_p50_ms": round(gaps_ms[len(gaps_ms) // 2], 3),
        "gap_p99_ms": round(gaps_ms[int(len(gaps_ms) * 0.99)], 3),
        "gap_mean_ms": round(statistics.fmean(gaps_ms), 3),
        "ledger_rows": len(ts),
    }
    h.delete_topics([topic])
    h.drop_schema(schema)
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--variants", default=",".join(VARIANTS))
    args = ap.parse_args()
    out = h.HERE / "results" / "m3_throughput.json"
    results = json.loads(out.read_text()) if out.exists() else []
    for run in range(1, args.runs + 1):
        for variant in args.variants.split(","):
            if any(r["variant"] == variant and r["run"] == run for r in results):
                continue  # resume after an interruption
            r = one_run(variant, run)
            results.append(r)
            print(json.dumps(r), flush=True)
            out.write_text(json.dumps(results, indent=1))
            time.sleep(5)  # give other benchmarks a chance at the lock


if __name__ == "__main__":
    main()
