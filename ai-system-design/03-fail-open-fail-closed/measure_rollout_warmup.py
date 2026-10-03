"""M3b: how much of "the global push was stopped at 56 ms" is the test harness?

The gate looks every 50 ms (rollout's poll) and each proxy reports its counters to Redis every
100 ms (proxy.FLUSH). In M3 every run warms up for exactly 0.5 s, so the doubled file always
lands at the same point of the proxies' report cycle. This repeats M3's two naive runs with
other warm-ups, which moves that point, and records at which check the gate said no.

10 runs per plan and warm-up. Writes results/m3b_rollout_warmup.csv and .json.
"""
import collections
import csv
import json
import logging
import os
import pathlib
import statistics
import sys

import lab
from config_holder import NaiveHolder
from generator import feature_names, grant_r0
from measure_rollout import one_run
from rollout import GLOBAL, STAGED

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))                 # the series folder
from common.measure_lock import measuring  # noqa: E402

RUNS = int(os.environ.get("RUNS", "10"))
WARMUPS = [0.5, 0.525, 0.55, 0.575, 1.0, 2.0]        # seconds of traffic before the bad file
PLANS = [("global", GLOBAL), ("staged", STAGED)]
POLL_MS = 50


def main() -> None:
    logging.getLogger("config").setLevel(logging.ERROR)
    with lab.connect() as conn:
        lab.reset_schema(conn)
        good = feature_names(conn, "feature_gen")
        grant_r0(conn, "feature_gen")                 # the permission change
        bad = feature_names(conn, "feature_gen")
        lab.reset_schema(conn)

    r = lab.redis_client()
    rows = []
    with measuring("fail-open-rollout-warmup", poll=0.0005):
        for warmup in WARMUPS:
            for plan, stages in PLANS:
                for run in range(1, RUNS + 1):
                    result = one_run(NaiveHolder, stages, bad, good, "errors and blocks", r, warmup)
                    rows.append({"plan": plan, "warmup_s": warmup, "run": run,
                                 "proxies_with_errors": result["proxies_with_errors"],
                                 "failed_requests": result["failed_requests"],
                                 "rollout_ms": result["rollout_ms"],
                                 "stopped_at_check": round(result["rollout_ms"] // POLL_MS)})
            print("warm-up", warmup, "done", flush=True)

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m3b_rollout_warmup.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {}
    for plan, _ in PLANS:
        mine = [row for row in rows if row["plan"] == plan]
        per_warmup = {}
        for warmup in WARMUPS:
            ms = [row["rollout_ms"] for row in mine if row["warmup_s"] == warmup]
            per_warmup[str(warmup)] = {"median_ms": statistics.median(ms), "min_ms": min(ms),
                                       "max_ms": max(ms)}
        ms = [row["rollout_ms"] for row in mine]
        checks = collections.Counter(row["stopped_at_check"] for row in mine)
        summary[plan] = {
            "runs": len(mine),
            "proxies_with_errors": sorted({row["proxies_with_errors"] for row in mine}),
            "failed_requests": {"median": statistics.median(r_["failed_requests"] for r_ in mine),
                                "min": min(r_["failed_requests"] for r_ in mine),
                                "max": max(r_["failed_requests"] for r_ in mine)},
            "rollout_ms": {"median": statistics.median(ms), "min": min(ms), "max": max(ms)},
            "runs_stopped_at_check": {str(k): checks[k] for k in sorted(checks)},
            "by_warmup": per_warmup,
        }
    (out / "m3b_rollout_warmup.json").write_text(json.dumps(
        {"runs_per_plan_and_warmup": RUNS, "warmups_s": WARMUPS, "gate_poll_ms": POLL_MS,
         "proxy_report_ms": 100, "plans": summary}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
