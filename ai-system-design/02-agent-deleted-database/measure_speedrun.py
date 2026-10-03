"""M4: a 200-delete speedrun. An agent that is allowed to delete rows in its own workspace
fires 200 deletes back to back. Five variants, RUNS times each:

    no gate             the agent's credential, nothing in between
    token bucket        the gate: 5 destructive calls at once, then 1 a minute
    watchdog            the gate with the bucket opened wide; every call goes to Kafka and
                        the kill switch trips above 20 destructive calls in 10 seconds
    watchdog, paced     the same, with 20 ms between calls (a model that has to think)
    bucket + watchdog   the default bucket and the watchdog together

How many deletes slip through while the watchdog reacts is a timing question, so the
script runs under the shared measure lock.

    python measure_speedrun.py   -> results/m4_speedrun.csv, results/m4_speedrun_summary.json
"""
import collections
import csv
import json
import pathlib
import statistics
import sys
import threading
import time
import uuid

import redis
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import killswitch  # noqa: E402
import lab  # noqa: E402
from executor import Executor  # noqa: E402
from gate import Denied, ToolGate  # noqa: E402

N, RUNS = 200, 20
WIDE_OPEN = dict(rate=1e6, burst=10**6)  # a bucket that never says no
VARIANTS = {  # name: (gate options or None for no gate, events to Kafka?, seconds between calls)
    "no gate": (None, False, 0.0),
    "token bucket": ({}, False, 0.0),
    "watchdog": (WIDE_OPEN, True, 0.0),
    "watchdog, paced": (WIDE_OPEN, True, 0.020),
    "bucket + watchdog": ({}, True, 0.0),
}


def speedrun(agent_id: str, call, pause: float) -> dict:
    refused = collections.Counter()
    started, call_21, switch_ms = time.perf_counter(), None, None
    for email_id in range(1, N + 1):
        if email_id == 21:
            call_21 = time.perf_counter()
        try:
            call(agent_id, f"DELETE FROM staging.inbox WHERE id = {email_id}")
        except Denied as denied:
            refused[denied.reason] += 1
            if denied.reason == "kill switch" and switch_ms is None:
                switch_ms = round((time.perf_counter() - call_21) * 1000, 2)
        if pause:
            time.sleep(pause)
    return {"elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
            "refused_rate_limit": refused["rate limit"],
            "refused_kill_switch": refused["kill switch"], "switch_ms": switch_ms}


def drain(producer: Producer, topic: str, r: redis.Redis) -> None:
    """Wait until the watchdog has read everything sent so far (one partition, in order)."""
    canary = f"canary-{uuid.uuid4().hex[:8]}"
    for _ in range(killswitch.MAX_DESTRUCTIVE + 1):
        producer.produce(topic, key=canary, value=json.dumps(
            {"agent": canary, "kind": "destructive", "ts": time.time()}))
    producer.flush(10)
    deadline = time.monotonic() + 30
    while not r.exists(f"killswitch:{canary}"):
        if time.monotonic() > deadline:
            raise RuntimeError("the watchdog fell behind")
        time.sleep(0.01)


def main() -> None:
    lab.create()
    lab.reset(staging=("inbox",))
    r = redis.Redis.from_url(lab.REDIS_URL)
    r.flushdb()
    topic = f"{lab.TOPIC_PREFIX}speedrun-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": lab.KAFKA})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(10)
    producer = Producer({"bootstrap.servers": lab.KAFKA})
    stopped, ready = threading.Event(), threading.Event()
    watchdog = threading.Thread(target=killswitch.watch, daemon=True, kwargs=dict(
        r=r, topic=topic, group=topic, stopped=stopped, ready=ready))
    watchdog.start()
    if not ready.wait(60):
        raise RuntimeError("the watchdog never got its partition")

    owner, agent = lab.connect(lab.OWNER), lab.connect(lab.AGENT)
    operator = lab.connect(lab.OPERATOR)
    executor = Executor(agent, operator, soft_delete=True)
    rows = []
    try:
        with measuring("agentdel-m4-speedrun"):
            for run in range(1, RUNS + 1):
                for name, (options, to_kafka, pause) in VARIANTS.items():
                    owner.execute("TRUNCATE staging.inbox")
                    lab.seed(owner, "staging", "inbox", N)
                    if options is None:
                        def call(agent_id, sql):
                            agent.execute(sql)
                    else:
                        audit = killswitch.audit_to_kafka(producer, topic) if to_kafka else (
                            lambda event: None)
                        gate = ToolGate(r, executor, audit=audit, **options)

                        def call(agent_id, sql, gate=gate):
                            gate.call(agent_id, "staging", sql)
                    result = speedrun(f"speedrunner-{uuid.uuid4().hex[:8]}", call, pause)
                    left = owner.execute("SELECT count(*) FROM staging.inbox").fetchone()[0]
                    rows.append({"run": run, "variant": name, "attempted": N,
                                 "deleted": N - left, **result})
                    if to_kafka:
                        drain(producer, topic, r)
                print(f"run {run}/{RUNS} done", flush=True)
    finally:
        stopped.set()
        watchdog.join(10)
        producer.flush(10)
        admin.delete_topics([topic])[topic].result(10)
        for conn in (owner, agent, operator):
            conn.close()
        r.flushdb()
        lab.reset()

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m4_speedrun.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    def spread(values: list) -> dict | None:
        values = [v for v in values if v is not None]
        return {"median": statistics.median(values), "min": min(values),
                "max": max(values)} if values else None

    summary = []
    for name in VARIANTS:
        group = [row for row in rows if row["variant"] == name]
        summary.append({"variant": name, "runs": len(group), "attempted": N,
                        **{key: spread([row[key] for row in group]) for key in (
                            "deleted", "refused_rate_limit", "refused_kill_switch",
                            "switch_ms", "elapsed_ms")}})
        print(summary[-1])
    (out / "m4_speedrun_summary.json").write_text(json.dumps(
        {"emails": N, "runs": RUNS, "max_destructive": killswitch.MAX_DESTRUCTIVE,
         "window_s": killswitch.WINDOW, "results": summary}, indent=2))


if __name__ == "__main__":
    main()
