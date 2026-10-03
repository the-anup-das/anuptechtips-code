"""M2: one doubled feature file, five kinds of consumer. Twelve proxies read the file from
Kafka, then each serves 1,000 requests (900 from humans, 100 from bots). Counts only, so no
measure lock.

Writes results/m2_consumers.csv and results/m2_consumers.json.
"""
import csv
import json
import logging
import pathlib
import time
import uuid

import lab
from config_holder import ConfigHolder, NaiveHolder, ScoreZeroHolder
from generator import feature_names, grant_r0
from proxy import start_fleet
from rollout import kafka_publisher

HERE = pathlib.Path(__file__).resolve().parent
RUNS, REQUESTS = 5, 1000
# outcome, holder, was a good file loaded first?, what to do when there is no usable file
OUTCOMES = [
    ("good file (baseline)", NaiveHolder, True, "open"),
    ("crash", NaiveHolder, True, "open"),
    ("fail wrong", ScoreZeroHolder, True, "open"),
    ("fail stale", ConfigHolder, True, "open"),
    ("fail open", ConfigHolder, False, "open"),
    ("fail closed", ConfigHolder, False, "closed"),
]


def deliver(fleet, version: int) -> None:
    """Poll until every proxy has looked at `version`."""
    deadline = time.monotonic() + 15
    while not all(proxy.holder.seen >= version for proxy in fleet):
        if time.monotonic() > deadline:
            raise TimeoutError(f"version {version} never arrived")
        for proxy in fleet:
            proxy.poll(0.01)


def one_run(outcome: str, holder, good_first: bool, on_unknown: str, good, bad) -> dict:
    run = uuid.uuid4().hex[:8]
    publish = kafka_publisher(run)
    fleet = start_fleet(holder, run, lab.topic_ends(), on_unknown)
    files = ([good] if good_first else []) + ([] if outcome.startswith("good") else [bad])
    for version, names in enumerate(files, start=1):
        for cohort in range(len(lab.COHORTS)):
            publish(cohort, version, names)
        deliver(fleet, version)
    for proxy in fleet:
        proxy.serve(REQUESTS)
        proxy.close()

    def total(fn) -> int:
        return sum(fn(proxy) for proxy in fleet)

    return {
        "outcome": outcome, "proxies": len(fleet), "requests": total(lambda p: p.sent),
        "status_200": total(lambda p: p.statuses[200]),
        "status_403": total(lambda p: p.statuses[403]),
        "status_500": total(lambda p: p.statuses[500]),
        "status_503": total(lambda p: p.statuses[503]),
        "humans_blocked": total(lambda p: p.humans_blocked),
        "bots_passed": total(lambda p: p.bots_passed),
        "stale_serves": total(lambda p: p.stale),
        "files_rejected": total(lambda p: p.holder.rejected),
    }


def main() -> None:
    logging.getLogger("config").setLevel(logging.ERROR)   # 12 rejections per run: keep it quiet
    with lab.connect() as conn:
        lab.reset_schema(conn)
        good = feature_names(conn, "feature_gen")
        grant_r0(conn, "feature_gen")                 # the permission change
        bad = feature_names(conn, "feature_gen")
        lab.reset_schema(conn)
    print(f"good file: {len(good)} features, doubled file: {len(bad)}")

    rows = []
    for run in range(1, RUNS + 1):
        for outcome, holder, good_first, on_unknown in OUTCOMES:
            rows.append({"run": run,
                         **one_run(outcome, holder, good_first, on_unknown, good, bad)})

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m2_consumers.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {}
    for outcome, *_ in OUTCOMES:
        mine = [r for r in rows if r["outcome"] == outcome]
        assert all(r == {**mine[0], "run": r["run"]} for r in mine), f"{outcome}: runs disagree"
        summary[outcome] = {k: v for k, v in mine[0].items() if k not in ("run", "outcome")}
    (out / "m2_consumers.json").write_text(json.dumps(
        {"runs": RUNS, "features_good": len(good), "features_bad": len(bad),
         "outcomes": summary}, indent=2))

    print(f"{'outcome':22} {'200':>6} {'403':>6} {'500':>6} {'503':>6} "
          f"{'humans blocked':>15} {'bots passed':>12} {'stale':>6}")
    for outcome, s in summary.items():
        print(f"{outcome:22} {s['status_200']:>6} {s['status_403']:>6} {s['status_500']:>6} "
              f"{s['status_503']:>6} {s['humans_blocked']:>15} {s['bots_passed']:>12} "
              f"{s['stale_serves']:>6}")


if __name__ == "__main__":
    main()
