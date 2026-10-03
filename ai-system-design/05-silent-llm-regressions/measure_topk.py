"""M2: the batch-dependent top-k bug and the fp16 pick, at batch sizes 1, 8, 32 and 64.

A benchmark that runs one request at a time is the batch-1 column. Each run sends the 200
known-answer probes in 48 sessions (9,600 requests) through three samplers and compares
every reply with the reference: exact top-k, fp32, batch size 1.

    python measure_topk.py [--runs 20]
"""
import argparse
import csv
import json
import statistics

import numpy as np

import harness
from evals import SYSTEM_PROMPT
from fake_model import Model, Request, Serving, approx_top_k, respond
from probes import by_suite, score

BATCH_SIZES = (1, 8, 32, 64)
SAMPLERS = {
    "exact top-k, fp32 pick": Serving(),
    "exact top-k, fp16 pick": Serving(dtype=np.float16),
    "approximate top-k (bug), fp32 pick": Serving(top_k=approx_top_k),
}
SESSIONS = 48
MODEL = Model()
RESULTS = harness.HERE / "results"


def replies(serving: Serving, requests: list[Request], batch_size: int) -> np.ndarray:
    return np.concatenate([respond(MODEL, serving, SYSTEM_PROMPT, requests[i:i + batch_size])
                           for i in range(0, len(requests), batch_size)])


def one_run(run: int) -> list[dict]:
    requests = [Request(probe, f"r{run}-s{s}") for s in range(SESSIONS)
                for probe in by_suite("known_answer")]
    reference = replies(Serving(), requests, 1)
    rows = []
    for name, serving in SAMPLERS.items():
        for batch_size in BATCH_SIZES:
            out = replies(serving, requests, batch_size)
            rows.append({
                "run": run, "sampler": name, "batch_size": batch_size, "requests": len(requests),
                "passed": sum(score(r.probe, t) for r, t in zip(requests, out)),
                "answers_changed": int((out[:, 0] != reference[:, 0]).sum()),
                "replies_changed": int((out != reference).any(axis=1).sum()),
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
    with open(RESULTS / "m2_topk_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {"runs": runs, "requests_per_run": rows[0]["requests"], "cells": []}
    for name in SAMPLERS:
        for batch_size in BATCH_SIZES:
            cell = [r for r in rows if r["sampler"] == name and r["batch_size"] == batch_size]
            entry = {"sampler": name, "batch_size": batch_size}
            for key in ("passed", "answers_changed", "replies_changed"):
                values = [r[key] for r in cell]
                entry[key] = {"median": statistics.median(values), "min": min(values),
                              "max": max(values)}
            entry["pass_rate_median"] = entry["passed"]["median"] / rows[0]["requests"]
            summary["cells"].append(entry)
    summary["setup"] = harness.MACHINE + "; Python 3.12, NumPy 1.26 (no services needed)"
    (RESULTS / "m2_topk_summary.json").write_text(json.dumps(summary, indent=2))
    for cell in summary["cells"]:
        print(f"{cell['sampler']:36s} batch {cell['batch_size']:2d}: pass "
              f"{cell['pass_rate_median']:.2%}, answers changed {cell['answers_changed']}, "
              f"replies changed {cell['replies_changed']}")


if __name__ == "__main__":
    main()
