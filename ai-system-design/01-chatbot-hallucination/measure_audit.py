"""M5: 300 replies written through the outbox, the relay killed mid-run, the audit trail
rebuilt from Kafka alone. Does it match Postgres?

Each run: answer all 300 questions with every gate on (30 conversations of 10 replies),
start a relay that dies right after the broker acknowledges its 6th batch of 25 and before
it marks that batch as published, start a second relay to finish, then read the topic from
the beginning, rebuild one record per audit id and compare every field with answer_audit.

20 runs, one topic per run (chatbot.audit.m5-*), deleted afterwards.
Writes results/m5_audit_runs.csv and results/m5_audit.json.
"""
import csv
import json
import pathlib
import statistics
import subprocess
import sys
import uuid

import psycopg
import redis

from db import DSN, REDIS_URL, reset
from fake_llm import FakeLLM
from questions import KEY, QUESTIONS
from rebuild import create_topic, delete_topics, mismatches, read_topic, rebuild
from service import handle

HERE = pathlib.Path(__file__).resolve().parent
RUNS, BATCH, CRASH_AFTER = 20, 25, 6


def relay(*extra: str) -> int:
    cmd = [sys.executable, "relay.py", "--batch", str(BATCH), "--until-empty", *extra]
    return subprocess.run(cmd, cwd=HERE, timeout=120).returncode


def one_run(conn: psycopg.Connection, r: redis.Redis, seed: int) -> dict:
    reset(conn)
    r.flushdb()
    topic = f"chatbot.audit.m5-{seed}-{uuid.uuid4().hex[:6]}"
    create_topic(topic)
    try:
        llm = FakeLLM(seed, KEY)
        for i, q in enumerate(QUESTIONS):
            handle(conn, r, llm, f"conv-{i % 30}", q.text, q.ticket, topic=topic)

        assert relay("--crash-after", str(CRASH_AFTER)) == 1       # dies holding batch 6
        pending = conn.execute(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
        assert relay() == 0                                         # the restart finishes the job

        events = read_topic(topic)
        trail = rebuild(events)
        return {"run": seed, "replies": len(QUESTIONS), "pending_after_crash": pending,
                "events_in_kafka": len(events), "duplicates": len(events) - len(trail),
                "rebuilt": len(trail), "mismatches": len(mismatches(conn, trail))}
    finally:
        delete_topics([topic])


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        r = redis.Redis.from_url(REDIS_URL)
        rows = [one_run(conn, r, seed) for seed in range(1, RUNS + 1)]
        r.flushdb()

    out = HERE / "results"
    with open(out / "m5_audit_runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = {"runs": RUNS, "batch": BATCH, "relay_killed_after_acked_batch": CRASH_AFTER}
    for m in list(rows[0])[1:]:
        values = [row[m] for row in rows]
        summary[m] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    (out / "m5_audit.json").write_text(json.dumps(summary, indent=2) + "\n")
    for m in list(rows[0])[1:]:
        print(f"{m:<40} median {summary[m]['median']:g}  min {summary[m]['min']}  "
              f"max {summary[m]['max']}")


if __name__ == "__main__":
    main()
