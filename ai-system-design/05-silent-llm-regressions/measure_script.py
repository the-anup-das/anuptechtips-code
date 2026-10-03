"""M3: the script detector, with the rare-script boost off and on.

Each run sends the 200 known-answer probes in 50 sessions (10,000 replies) through the
healthy sampler and through one with script_boost on, then counts the replies the detector
flags and the replies the known-answer eval still passes.

    python measure_script.py [--runs 20]
"""
import argparse
import csv
import json
import statistics

import numpy as np

import harness
from detectors import unexpected_scripts
from evals import SYSTEM_PROMPT
from fake_model import Model, Request, Serving, respond
from probes import by_suite, render, score

SESSIONS, BATCH = 50, 8
BOOST = 12.0
MODEL = Model()
RESULTS = harness.HERE / "results"


def one_run(run: int) -> list[dict]:
    requests = [Request(probe, f"r{run}-s{s}") for s in range(SESSIONS)
                for probe in by_suite("known_answer")]
    rows = []
    for bug, serving in (("off", Serving()), ("on", Serving(script_boost=BOOST))):
        out = np.concatenate([respond(MODEL, serving, SYSTEM_PROMPT, requests[i:i + BATCH])
                              for i in range(0, len(requests), BATCH)])
        flagged = [bool(unexpected_scripts(r.probe.prompt, render(t)))
                   for r, t in zip(requests, out)]
        passed = [score(r.probe, t) for r, t in zip(requests, out)]
        rows.append({
            "run": run, "bug": bug, "replies": len(requests),
            "flagged": sum(flagged),
            "flagged_translation_probes": sum(f for f, r in zip(flagged, requests)
                                              if r.probe.foreign),
            "flagged_other_probes": sum(f for f, r in zip(flagged, requests)
                                        if not r.probe.foreign),
            "passed": sum(passed),
            "flagged_but_passed": sum(f and p for f, p, r in zip(flagged, passed, requests)
                                      if not r.probe.foreign),
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    runs = parser.parse_args().runs
    rows = []
    for run in range(runs):
        rows += one_run(run)
        print(f"run {run} done")
    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / "m3_script_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {"runs": runs, "replies_per_run": rows[0]["replies"], "boost": BOOST}
    for bug in ("off", "on"):
        cell = [r for r in rows if r["bug"] == bug]
        summary[f"bug_{bug}"] = {
            key: {"median": statistics.median(r[key] for r in cell),
                  "min": min(r[key] for r in cell), "max": max(r[key] for r in cell)}
            for key in rows[0] if key not in ("run", "bug", "replies")}
    summary["setup"] = harness.MACHINE + "; Python 3.12, NumPy 1.26 (no services needed)"
    (RESULTS / "m3_script_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
