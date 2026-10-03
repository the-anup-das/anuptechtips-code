"""M5: what the gate adds to one tool call.

The executor is a no-op, so each number is the gate's own work: classifying the call and
its Redis round trips. ROUNDS x CALLS calls per kind, one at a time, from Python on the
host to Redis in Docker. Approving is the human's side, so approve() is timed separately
and not counted inside the approved calls. Timing, so it runs under the shared measure lock.

    python measure_gate_overhead.py   -> results/m5_gate_overhead_rounds.csv,
                                         results/m5_gate_overhead.json
"""
import csv
import json
import pathlib
import statistics
import sys
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
from gate import Denied, ToolGate, call_digest  # noqa: E402

ROUNDS, CALLS, WARMUP = 20, 1000, 100
AGENT = "bench"
UPDATE = "UPDATE prod.executives SET name = 'x' WHERE id = 1"
DROP = "DROP TABLE prod.executives"


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


def timed(fn, before=None) -> float:
    if before:
        before()
    start = time.perf_counter_ns()
    fn()
    return (time.perf_counter_ns() - start) / 1000  # microseconds


def main() -> None:
    r = redis.Redis.from_url(lab.REDIS_URL)
    r.flushdb()
    gate = ToolGate(r, lambda env, kind, sql, max_rows: None, rate=1e6, burst=10**6)
    topic = f"{lab.TOPIC_PREFIX}bench-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": lab.KAFKA})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(10)
    producer = Producer({"bootstrap.servers": lab.KAFKA})
    audited = ToolGate(r, lambda env, kind, sql, max_rows: None,
                       audit=killswitch.audit_to_kafka(producer, topic), rate=1e6, burst=10**6)

    def denied():
        try:
            gate.call(AGENT, "prod", DROP, 1)  # max_rows=1: a digest nobody ever approves
        except Denied:
            pass

    cases = {  # name: (the timed call, untimed preparation, Redis round trips)
        "Redis PING": (r.ping, None, 1),
        "read on prod": (lambda: gate.call(AGENT, "prod", "SELECT 1"), None, 1),
        "destructive on staging": (
            lambda: gate.call(AGENT, "staging", "DELETE FROM staging.inbox WHERE id = 1"), None, 2),
        "approved write on prod": (
            lambda: gate.call(AGENT, "prod", UPDATE),
            lambda: gate.approve(call_digest(AGENT, "prod", UPDATE), "anup"), 3),
        "approved destructive on prod": (
            lambda: gate.call(AGENT, "prod", DROP),
            lambda: gate.approve(call_digest(AGENT, "prod", DROP), "anup"), 4),
        "denied destructive on prod": (denied, None, 4),
        "approved destructive on prod + Kafka audit": (
            lambda: audited.call(AGENT, "prod", DROP),
            lambda: audited.approve(call_digest(AGENT, "prod", DROP), "anup"), 4),
        "approve() (the human's side)": (
            lambda: gate.approve(uuid.uuid4().hex, "anup"), None, 1),
    }

    rounds, samples = [], {name: [] for name in cases}
    with measuring("agentdel-m5-gate-overhead"):
        for round_no in range(1, ROUNDS + 1):
            for name, (fn, before, _) in cases.items():
                for _ in range(WARMUP):
                    timed(fn, before)
                batch = [timed(fn, before) for _ in range(CALLS)]
                samples[name] += batch
                rounds.append({"round": round_no, "case": name,
                               "p50_us": round(percentile(batch, 0.50), 1),
                               "p99_us": round(percentile(batch, 0.99), 1)})
            print(f"round {round_no}/{ROUNDS} done", flush=True)
    producer.flush(10)
    admin.delete_topics([topic])[topic].result(10)
    r.flushdb()

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m5_gate_overhead_rounds.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rounds[0]))
        writer.writeheader()
        writer.writerows(rounds)
    summary = []
    for name, (_, _, round_trips) in cases.items():
        medians = [row["p50_us"] for row in rounds if row["case"] == name]
        summary.append({"case": name, "redis_round_trips": round_trips,
                        "calls": len(samples[name]),
                        "p50_us": round(percentile(samples[name], 0.50), 1),
                        "p99_us": round(percentile(samples[name], 0.99), 1),
                        "round_p50_min_us": min(medians), "round_p50_max_us": max(medians),
                        "mean_us": round(statistics.fmean(samples[name]), 1)})
        print(summary[-1])
    (out / "m5_gate_overhead.json").write_text(json.dumps(
        {"rounds": ROUNDS, "calls_per_round": CALLS, "warmup": WARMUP, "results": summary},
        indent=2))


if __name__ == "__main__":
    main()
