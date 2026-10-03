"""M4: prompt ablation. Run the 350-probe suite on a six-line system prompt, then on the
prompt with each line removed, for two model versions. One of the lines caps the words
between tool calls at 25.

Every result is written to eval_runs in Postgres and read back before it is compared.

    python measure_ablation.py [--runs 20]
"""
import argparse
import csv
import json
import statistics

import psycopg

import evals
import harness
from ablation import ablate, harmful_lines
from evals import CAP_LINE, SYSTEM_PROMPT
from fake_model import Model, fingerprint
from probes import SUITES

PROMPT = SYSTEM_PROMPT[:3] + [CAP_LINE] + SYSTEM_PROMPT[3:]
MODELS = (Model("model-1", skill=4.6), Model("model-2", skill=4.9))
SESSIONS = 20
RESULTS = harness.HERE / "results"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    runs = parser.parse_args().runs
    dsn = harness.setup_schema("m4_ablation")
    rows, flagged = [], []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for run in range(runs):
            sessions = [f"r{run}-s{i}" for i in range(SESSIONS)]
            for model in MODELS:
                for line, scores in ablate(model, PROMPT, sessions).items():
                    prompt = [p for p in PROMPT if p != line]
                    evals.save(conn, f"m4-r{run}", model, prompt, scores)
                results = {line: evals.load(conn, f"m4-r{run}", model.version,
                                            fingerprint([p for p in PROMPT if p != line]))
                           for line in [None, *PROMPT]}
                flagged.append({"run": run, "model": model.version,
                                "harmful": harmful_lines(results)})
                for line, scores in results.items():
                    for suite in SUITES:
                        rows.append({"run": run, "model": model.version,
                                     "removed_line": line or "(none: the full prompt)",
                                     "suite": suite, "passed": scores[suite].passed,
                                     "total": scores[suite].total})
            print(f"run {run}: {[f['harmful'] for f in flagged[-2:]]}")
    harness.drop_schema("m4_ablation")

    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / "m4_ablation_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    def rate_stats(model: str, line: str, suites: tuple[str, ...]) -> dict:
        per_run = []
        for run in range(runs):
            cell = [r for r in rows if r["run"] == run and r["model"] == model
                    and r["removed_line"] == line and r["suite"] in suites]
            per_run.append(sum(r["passed"] for r in cell) / sum(r["total"] for r in cell))
        return {"median": statistics.median(per_run), "min": min(per_run), "max": max(per_run)}

    summary = {"runs": runs, "requests_per_eval": 350 * SESSIONS, "prompt": PROMPT, "models": {}}
    for model in MODELS:
        lines = {}
        for line in ["(none: the full prompt)", *PROMPT]:
            lines[line] = {"overall": rate_stats(model.version, line, SUITES),
                           **{suite: rate_stats(model.version, line, (suite,)) for suite in SUITES},
                           "runs_flagged_harmful": sum(
                               1 for f in flagged if f["model"] == model.version
                               and any(h[0] == line for h in f["harmful"]))}
        summary["models"][model.version] = lines
    summary["flags_per_run"] = [{"run": f["run"], "model": f["model"],
                                 "harmful": [list(h) for h in f["harmful"]]} for f in flagged]
    summary["setup"] = harness.MACHINE + "; Python 3.12, NumPy 1.26, psycopg 3.3.6, PostgreSQL 18.6"
    (RESULTS / "m4_ablation_summary.json").write_text(json.dumps(summary, indent=2))
    for model, lines in summary["models"].items():
        for line, cell in lines.items():
            print(f"{model} | removed: {line[:44]:44s} | overall {cell['overall']['median']:.2%} "
                  f"| multi-step {cell['multi_step']['median']:.2%} "
                  f"| flagged in {cell['runs_flagged_harmful']}/{runs} runs")


if __name__ == "__main__":
    main()
