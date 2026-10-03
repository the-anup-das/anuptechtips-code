"""M3: one doubled feature file, pushed to twelve proxies two ways: to everyone at once
(GLOBAL) or cohort by cohort (STAGED). Both plans get the same health gate and the same
automatic rollback, so the only difference is who receives the file first.

Each run: twelve proxies (threads, 100 requests a second each) load a good file from Kafka,
serve for half a second, then the next file is rolled out. 20 runs per variant.

Writes results/m3_rollout_runs.csv and results/m3_rollout_summary.json.
"""
import csv
import functools
import json
import logging
import os
import pathlib
import statistics
import sys
import time
import uuid

import lab
from config_holder import ConfigHolder, NaiveHolder, ScoreZeroHolder
from generator import feature_names, grant_r0
from proxy import ProxyThread, start_fleet
from rollout import GLOBAL, STAGED, health, kafka_publisher, rollout

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))                 # the series folder
from common.measure_lock import measuring  # noqa: E402

RUNS = int(os.environ.get("RUNS", "20"))
# consumers, holder, plan, stages, file, gate
VARIANTS = [
    ("naive", NaiveHolder, "global", GLOBAL, "bad", "errors and blocks"),
    ("naive", NaiveHolder, "staged", STAGED, "bad", "errors and blocks"),
    ("hardened", ConfigHolder, "global", GLOBAL, "bad", "errors and blocks"),
    ("hardened", ConfigHolder, "staged", STAGED, "bad", "errors and blocks"),
    ("fail wrong", ScoreZeroHolder, "staged", STAGED, "bad", "errors and blocks"),
    ("fail wrong", ScoreZeroHolder, "staged", STAGED, "bad", "errors only"),
    ("naive", NaiveHolder, "global", GLOBAL, "good", "errors and blocks"),
    ("naive", NaiveHolder, "staged", STAGED, "good", "errors and blocks"),
]
METRICS = ["stages_passed", "proxies_with_errors", "failed_requests", "proxies_on_new_file",
           "proxies_serving_stale", "stale_serves", "humans_blocked", "rollout_ms",
           "first_error_ms", "last_error_ms"]


def ms(seconds: float) -> float:
    return round(seconds * 1000, 1)


def one_run(holder, stages, names, good, gate_kind, r, warmup: float = 0.5) -> dict:
    run = uuid.uuid4().hex[:8]
    publish = kafka_publisher(run)
    fleet = start_fleet(holder, run, lab.topic_ends(), epoch=time.perf_counter())
    threads = [ProxyThread(proxy) for proxy in fleet]
    for thread in threads:
        thread.start()
    for cohort in range(len(lab.COHORTS)):
        publish(cohort, 1, good)
    while not all(proxy.holder.version == 1 for proxy in fleet):
        time.sleep(0.005)
    for thread in threads:
        thread.serving.set()
    time.sleep(warmup)                                # normal traffic on the good file

    max_blocked = 1.0 if gate_kind == "errors only" else 0.5
    gate = functools.partial(health, r, run, max_blocked=max_blocked)
    blocked_before = sum(proxy.humans_blocked for proxy in fleet)
    started = time.perf_counter()
    passed = rollout(2, names, good, publish, gate, stages=stages)
    finished = time.perf_counter()
    time.sleep(0.5)                                   # let a rollback land
    for thread in threads:
        thread.stop()

    failing = [proxy for proxy in fleet if proxy.statuses[500]]
    return {
        "stages_passed": passed,
        "proxies_with_errors": len(failing),
        "failed_requests": sum(proxy.statuses[500] for proxy in fleet),
        "proxies_on_new_file": sum(proxy.holder.version == 2 for proxy in fleet),
        "proxies_serving_stale": sum(proxy.stale > 0 for proxy in fleet),
        "stale_serves": sum(proxy.stale for proxy in fleet),
        "humans_blocked": sum(proxy.humans_blocked for proxy in fleet) - blocked_before,
        # from the first publish of the new file until rollout() returned: the gate's
        # verdict plus the rollback for a bad file, every stage's soak for a good one
        "rollout_ms": round((finished - started) * 1000, 1),
        "first_error_ms": ms(min(p.first_error for p in failing) - started) if failing else "",
        "last_error_ms": ms(max(p.last_error for p in failing) - started) if failing else "",
    }


def summarize(rows: list[dict]) -> dict:
    out = {"runs": len(rows)}
    for metric in METRICS:
        values = [row[metric] for row in rows if row[metric] != ""]
        if values:
            out[metric] = {"median": statistics.median(values), "min": min(values),
                           "max": max(values)}
    return out


def main() -> None:
    logging.getLogger("config").setLevel(logging.ERROR)
    with lab.connect() as conn:
        lab.reset_schema(conn)
        good = feature_names(conn, "feature_gen")
        grant_r0(conn, "feature_gen")                 # the permission change
        bad = feature_names(conn, "feature_gen")
        lab.reset_schema(conn)

    r = lab.redis_client()
    r.flushdb()
    rows = []
    # a short poll, so a queue of other benchmarks on this machine can't starve this one
    with measuring("fail-open-rollout", poll=0.0005):
        for consumers, holder, plan, stages, file, gate_kind in VARIANTS:
            for run in range(1, RUNS + 1):
                names = bad if file == "bad" else good
                rows.append({"consumers": consumers, "plan": plan, "file": file,
                             "gate": gate_kind, "run": run,
                             **one_run(holder, stages, names, good, gate_kind, r)})
            print(consumers, plan, file, gate_kind, "done", flush=True)

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m3_rollout_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {}
    for consumers, _, plan, _, file, gate_kind in VARIANTS:
        mine = [row for row in rows if (row["consumers"], row["plan"], row["file"], row["gate"])
                == (consumers, plan, file, gate_kind)]
        summary[f"{consumers} / {plan} / {file} file / gate: {gate_kind}"] = summarize(mine)
    (out / "m3_rollout_summary.json").write_text(json.dumps(
        {"proxies": sum(lab.COHORTS), "cohorts": lab.COHORTS, "requests_per_second_per_proxy": 100,
         "soak_seconds_per_stage": 1.0, "features_good": len(good), "features_bad": len(bad),
         "variants": summary}, indent=2))

    for name, s in summary.items():
        print(name)
        for metric in METRICS:
            if metric in s:
                m = s[metric]
                print(f"    {metric:22} median {m['median']:>8}  min {m['min']:>8}  "
                      f"max {m['max']:>8}")


if __name__ == "__main__":
    main()
