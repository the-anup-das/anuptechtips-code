"""M2: the abstain threshold. Sweep tau and watch what it buys and what it costs, with the
threshold as the only safety net (scope) and with the answer check behind it (all gates).

Same 300 questions and 20 seeds as M1. Writes results/m2_tau_sweep.csv and
results/m2_tau_sweep.json.
"""
import csv
import json
import pathlib

import psycopg

from bot import ALL_GATES
from db import DSN, reset
from measure_gates import SCOPE, SEEDS, count, stats
from measure_gates import run as run_gates

HERE = pathlib.Path(__file__).resolve().parent
TAUS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
CONFIGS = {"scope": SCOPE, "all gates": ALL_GATES}
METRICS = ("unsafe", "false_handoffs", "handoffs", "drafted", "tier2")


def main() -> None:
    summary, lines = {}, []
    with psycopg.connect(DSN, autocommit=True) as conn:
        reset(conn)
        for tau in TAUS:
            runs = count(list(run_gates(conn, tau=tau, configs=CONFIGS)))
            for config in CONFIGS:
                per_seed = [runs[(seed, config)] for seed in SEEDS]
                s = {m: stats([c[m] for c in per_seed]) for m in METRICS}
                summary.setdefault(config, {})[f"{tau:.1f}"] = s
                lines += [[config, tau, seed, *(c[m] for m in METRICS)]
                          for seed, c in zip(SEEDS, per_seed)]

    out = HERE / "results"
    with open(out / "m2_tau_sweep.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["config", "tau", "seed", *METRICS])
        w.writerows(lines)
    (out / "m2_tau_sweep.json").write_text(json.dumps(
        {"taus": TAUS, "seeds": len(SEEDS), "medians_min_max": summary}, indent=2) + "\n")

    print(f"{'tau':>4} | {'scope only: unsafe':>17} {'false hand-offs':>16} {'drafted':>8} | "
          f"{'all gates: unsafe':>18} {'false hand-offs':>16} {'drafted':>8}")
    for tau in TAUS:
        a, b = summary["scope"][f"{tau:.1f}"], summary["all gates"][f"{tau:.1f}"]
        print(f"{tau:>4} | {a['unsafe']['median']:>17g} {a['false_handoffs']['median']:>16g} "
              f"{a['drafted']['median']:>8g} | {b['unsafe']['median']:>18g} "
              f"{b['false_handoffs']['median']:>16g} {b['drafted']['median']:>8g}")


if __name__ == "__main__":
    main()
