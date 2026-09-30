"""M1b: kill -9 relay workers mid-run and count the duplicates that reach Kafka.

Each run: fresh schema, 400,000 pending rows, 4 relay_worker.py processes with
the KafkaPublisher (batch 100). Ten times, at random 0.2-0.6 s intervals, a
random worker is killed (TerminateProcess on Windows, the equivalent of
SIGKILL) and a fresh one started. Afterwards the whole topic is read back and
every outbox id counted: missing = ids never seen, duplicates = extra copies.

    python measure_crash.py --runs 5
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import random
import sys
import time
import uuid

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, KAFKA, reset_schema  # noqa: E402
from harness import finish, go, seed, spawn  # noqa: E402

ROWS, WORKERS, BATCH, KILLS = 400_000, 4, 100, 10


def read_topic(topic: str) -> collections.Counter:
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"m1-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    seen: collections.Counter = collections.Counter()
    done = {p: ends[p] == 0 for p in parts}
    while not all(done.values()):
        for m in c.consume(num_messages=10_000, timeout=1.0):
            if m.error():
                continue
            seen[dict(m.headers())["id"].decode()] += 1
            if m.offset() + 1 >= ends[m.partition()]:
                done[m.partition()] = True
    c.close()
    return seen


def one_run(run: int, rng: random.Random) -> dict:
    reset_schema()
    prefix = f"outbox.m1-crash-{uuid.uuid4().hex[:6]}."
    topic = prefix + "order"
    with psycopg.connect(DSN, autocommit=True) as conn:
        seed(conn, ROWS)
        conn.execute("ANALYZE outbox")
        workers = [spawn(BATCH, publisher="kafka", topic_prefix=prefix) for _ in range(WORKERS)]
        go(workers)
        kills = 0
        while kills < KILLS:
            time.sleep(rng.uniform(0.2, 0.6))
            i = rng.randrange(WORKERS)
            if workers[i].poll() is not None:
                break  # ran out of work before the tenth kill
            workers[i].kill()
            workers[i].wait()
            kills += 1
            workers[i] = spawn(BATCH, publisher="kafka", topic_prefix=prefix, wait_for_go=False)
        for w in workers:
            finish(w, timeout=600)
        # a worker may have exited while a peer was restarting; one last sweep
        finish(spawn(BATCH, publisher="kafka", topic_prefix=prefix, wait_for_go=False))
        left = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
        ids = {str(r[0]) for r in conn.execute("SELECT id FROM outbox")}
    seen = read_topic(topic)
    AdminClient({"bootstrap.servers": KAFKA}).delete_topics([topic])
    dup_per_id = collections.Counter(seen.values())
    return {"run": run, "rows": ROWS, "kills": kills, "left_unpublished": left,
            "messages_in_topic": sum(seen.values()), "missing": len(ids - set(seen)),
            "duplicates": sum(c - 1 for c in seen.values()),
            "max_copies_of_one_id": max(seen.values()),
            "ids_by_copies": dict(sorted(dup_per_id.items()))}


def main() -> None:
    global ROWS
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--rows", type=int, default=ROWS)
    ap.add_argument("--out", default="m1_crash.json")
    a = ap.parse_args()
    ROWS = a.rows
    rng = random.Random(2026)
    rows = []
    with measuring("outbox-m1-crash"):  # correctness count, but it loads the box: be polite
        for run in range(1, a.runs + 1):
            r = one_run(run, rng)
            rows.append(r)
            print(r, flush=True)
    kills = sum(r["kills"] for r in rows)
    dups = sum(r["duplicates"] for r in rows)
    summary = {"runs": rows, "total_kills": kills, "total_duplicates": dups,
               "total_missing": sum(r["missing"] for r in rows),
               "duplicates_per_kill": round(dups / kills, 1) if kills else None,
               "batch": BATCH, "workers": WORKERS, "rows_per_run": ROWS}
    (HERE / "results" / a.out).write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "runs"}, indent=2))


if __name__ == "__main__":
    main()
