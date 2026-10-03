"""M1: sticky misrouting ramps from 0.8% to 16% of sessions. Global vs misrouted-slice pass
rate per tick, and the tick at which a sliced alert and a global alert first fire.

Every run sends 60 ticks x 2,000 probes through serve() -> Kafka -> the consumer -> Postgres,
then asks slices.degraded_slices() what it would have said at the end of each tick.

    python measure_misrouting.py [--runs 20]
"""
import argparse
import csv
import json
import statistics
import uuid

import psycopg
from confluent_kafka import Producer

import harness
import pipeline
from evals import SYSTEM_PROMPT, evaluate
from fake_model import Model
from probes import by_suite
from serving import Fleet, serve, traffic
from slices import DIMENSIONS, degraded_slices

TICKS, ONSET, CHANGE, PEAK = 60, 10, 40, 50   # bug appears; load-balancer change; ramp ends
LOW, HIGH = 0.008, 0.16
WINDOW = 3                                     # ticks an alert looks back over
USERS, PER_SESSION = 500, 4
MODEL = Model()
RESULTS = harness.HERE / "results"


def share(tick: int) -> float:
    """Share of new sessions the router sends to the wrong pool at this tick."""
    if tick < ONSET:
        return 0.0
    if tick < CHANGE:
        return LOW
    return min(HIGH, LOW + (tick - CHANGE + 1) / (PEAK - CHANGE) * (HIGH - LOW))


def first_alert(conn, baseline: float, by: tuple[str, ...], key: tuple[str, ...]) -> int | None:
    for tick in range(TICKS):
        window = (max(0, tick - WINDOW + 1), tick)
        if any(a.key == key for a in degraded_slices(conn, "known_answer", baseline, by=by,
                                                     ticks=window)):
            return tick
    return None


def one_run(run: int, baseline: float) -> tuple[dict, list[dict]]:
    name = f"m1_{run:02d}_{uuid.uuid4().hex[:6]}"
    topic = f"regress.{name}.events"
    dsn = harness.setup_schema(name)
    harness.create_topic(topic)
    thread, stop = harness.start_consumer(topic, name, dsn)
    producer = Producer({"bootstrap.servers": pipeline.BROKERS})
    fleet, sent = Fleet(), 0
    try:
        for tick in range(TICKS):
            fleet.misroute_share = share(tick)
            requests = traffic(tick, USERS, PER_SESSION, prefix=f"r{run}-")
            events = serve(fleet, MODEL, SYSTEM_PROMPT, requests, tick, run=name)
            pipeline.publish(producer, events, topic)
            sent += len(events)
        harness.wait_for_events(dsn, sent)
    finally:
        stop.set()
        thread.join(15)
        del producer    # a live producer would re-create the topic after it is deleted below

    with psycopg.connect(dsn, autocommit=True) as conn:
        counted = conn.execute("SELECT sum(total) FROM slice_stats").fetchone()[0]
        per_tick = conn.execute(
            "SELECT tick, sum(total), sum(passed), "
            "coalesce(sum(total) FILTER (WHERE pool = 'long-context'), 0), "
            "coalesce(sum(passed) FILTER (WHERE pool = 'long-context'), 0) "
            "FROM slice_stats GROUP BY tick ORDER BY tick").fetchall()
        sliced = first_alert(conn, baseline, ("pool",), ("long-context",))
        overall = first_alert(conn, baseline, (), ())
        # alerts that would have been wrong: anything before the bug, and any slice but the bad one
        false_alarms = 0
        for tick in range(TICKS):
            window = (max(0, tick - WINDOW + 1), tick)
            for dim in DIMENSIONS:
                hits = degraded_slices(conn, "known_answer", baseline, by=(dim,), ticks=window)
                false_alarms += sum(1 for a in hits if tick < ONSET or
                                    (dim == "pool" and a.key != ("long-context",)))
    harness.drop_schema(name)
    harness.delete_topic(topic)

    def rate(lo: int, hi: int, col: int) -> float:
        rows = [r for r in per_tick if lo <= r[0] < hi]
        return sum(r[col + 1] for r in rows) / sum(r[col] for r in rows)

    def users(sessions: set[str]) -> set[str]:
        return {s.split("-t")[0] for s in sessions}

    lost = {s for s, pool in fleet.sticky.items() if pool == "long-context"}
    early = {s for s in lost if int(s.split("-t")[1]) < CHANGE}
    row = {
        "run": run, "events_sent": sent, "events_counted": counted,
        "slice_alert_tick": sliced, "global_alert_tick": overall, "false_alarms": false_alarms,
        "global_rate_healthy": rate(0, ONSET, 1), "global_rate_low": rate(ONSET, CHANGE, 1),
        "global_rate_peak": rate(PEAK, TICKS, 1),
        "slice_rate_low": rate(ONSET, CHANGE, 3), "slice_rate_peak": rate(PEAK, TICKS, 3),
        "requests_misrouted_low": sum(r[3] for r in per_tick if ONSET <= r[0] < CHANGE)
        / sum(r[1] for r in per_tick if ONSET <= r[0] < CHANGE),
        "users_hit_by_change": len(users(early)) / USERS,
        "users_hit_by_end": len(users(lost)) / USERS,
    }
    ticks = [{"run": run, "tick": t, "share": share(t), "total": n, "passed": p,
              "slice_total": sn, "slice_passed": sp} for t, n, p, sn, sp in per_tick]
    return row, ticks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    runs = parser.parse_args().runs
    baseline_run = evaluate(MODEL, SYSTEM_PROMPT, [f"base{i}" for i in range(250)],
                            probes=by_suite("known_answer"))["known_answer"]   # 50,000 replies
    baseline = baseline_run.value
    rows, ticks = [], []
    for run in range(runs):
        row, per_tick = one_run(run, baseline)
        rows.append(row)
        ticks += per_tick
        print(f"run {run:2d}: slice alert at tick {row['slice_alert_tick']}, global alert at tick "
              f"{row['global_alert_tick']}, false alarms {row['false_alarms']}, "
              f"counted {row['events_counted']}/{row['events_sent']}")

    RESULTS.mkdir(exist_ok=True)
    for name, data in (("m1_misrouting_runs.csv", rows), ("m1_misrouting_ticks.csv", ticks)):
        with open(RESULTS / name, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0]))
            writer.writeheader()
            writer.writerows(data)

    def stats(key: str) -> dict:
        values = [r[key] for r in rows if r[key] is not None]
        return {"median": statistics.median(values), "min": min(values), "max": max(values),
                "runs_with_value": len(values)}

    setup = (harness.MACHINE + "; Python 3.12, NumPy 1.26, confluent-kafka 2.15.1, "
             "psycopg 3.3.6, Kafka 4.3.1, PostgreSQL 18.6")
    summary = {
        "runs": runs, "ticks": TICKS, "requests_per_tick": USERS * PER_SESSION,
        "schedule": {"bug_starts_at_tick": ONSET, "share_until_change": LOW,
                     "load_balancer_change_at_tick": CHANGE,
                     "share_by_tick": {str(t): round(share(t), 4) for t in range(CHANGE, PEAK)}},
        "alert": {"window_ticks": WINDOW, "z": 3.0, "tolerance": 0.01, "min_n": 30},
        "baseline": {"passed": baseline_run.passed, "total": baseline_run.total, "rate": baseline},
        **{key: stats(key) for key in rows[0] if key != "run"},
        "setup": setup,
    }
    (RESULTS / "m1_misrouting_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
