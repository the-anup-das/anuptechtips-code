"""M2: a slow consumer is evicted from the group (max.poll.interval.ms) and finishes anyway.

    python measure_rebalance.py --variant b --runs 5

Per run: 5,000 events on 6 partitions, three consumer processes in one group,
max.poll.interval.ms=10000. One event in 500 stalls for 12 s the first time any consumer
handles it, so its consumer misses the poll deadline, leaves the group, and its partition
moves to another consumer, which gets the uncommitted event again. Correctness counts
only, so no measure lock.
"""
import argparse
import json
import re
import time
import uuid

import psycopg

import harness as h

N_EVENTS = 5_000
SLOW_EVERY = 500
SLOW_S = 12
CONSUMERS = 3


def one_run(variant: str, run: int) -> dict:
    schema = f"m2_{variant}_{run}"
    dsn = h.setup_schema(schema, "CREATE TABLE slow_once (event_id text PRIMARY KEY)")
    topic = f"consumer.m2.{variant}.{run}.{uuid.uuid4().hex[:6]}"
    h.create_topic(topic, 6)
    h.produce_events(topic, N_EVENTS)
    logs = [h.HERE / "results" / "logs" / f"m2_{variant}_{run}_{i}.log" for i in range(CONSUMERS)]
    for log in logs:
        log.unlink(missing_ok=True)
    args = ["--slow-every", str(SLOW_EVERY), "--slow-seconds", str(SLOW_S),
            "--max-poll-interval-ms", "10000", "--session-timeout-ms", "6000"]
    t0 = time.time()
    procs = [h.start_worker(variant, topic, topic, dsn, log, *args) for log in logs]
    last, stable_since = -1, time.time()
    while True:
        time.sleep(1)
        cur = h.ledger_max_id(dsn)
        if cur != last:
            last, stable_since = cur, time.time()
        with psycopg.connect(dsn) as c:
            slow_seen = c.execute("SELECT count(*) FROM slow_once").fetchone()[0]
        # done: every slow event was hit, nothing new for longer than a stall, and caught up
        if slow_seen == N_EVENTS // SLOW_EVERY and time.time() - stable_since > SLOW_S + 5 \
                and h.lag(topic, topic) == 0:
            break
        if time.time() - t0 > 900:
            raise RuntimeError("run did not finish in 15 minutes")
    for p in procs:
        p.kill()
        p.wait()
    text = "\n".join(log.read_text(encoding="utf-8", errors="replace") for log in logs)
    e = h.effects(dsn)
    result = {
        "variant": variant, "run": run, "events": N_EVENTS, "consumers": CONSUMERS,
        "slow_events": N_EVENTS // SLOW_EVERY,
        "duplicates": e["duplicates"], "lost": N_EVENTS - e["distinct"],
        "balance_total": e["balance_total"], "expected_balance": N_EVENTS * h.AMOUNT,
        "evictions": text.count("|MAXPOLL|"),  # librdkafka: "...exceeded...: leaving group"
        "offset_commit_failed": text.count("offset commit failed"),
        "skipped_duplicates": text.count("skipped duplicate"),
        "wall_s": round(time.time() - t0, 1),
    }
    m = re.search(r"offset commit failed: (.*)", text)
    result["example_commit_error"] = m.group(1).strip() if m else None
    m = re.search(r"^.*\|MAXPOLL\|.*$", text, re.M)
    result["example_max_poll_log"] = m.group(0).strip() if m else None
    m = re.search(r"consumer error: (.*_MAX_POLL_EXCEEDED.*)", text)
    result["example_poll_error"] = m.group(1).strip() if m else None
    h.delete_topics([topic])
    h.drop_schema(schema)
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", required=True, choices=["b", "c"])
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()
    out = h.HERE / "results" / f"m2_rebalance_{args.variant}.json"
    results = []
    for run in range(1, args.runs + 1):
        r = one_run(args.variant, run)
        results.append(r)
        print(json.dumps(r), flush=True)
        out.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
