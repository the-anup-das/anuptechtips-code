"""M5: the sycophancy trap. Four candidates differ only in agree_bias. Simulated users pick
the one they thumb up most, the offline suite and a 2% A/B test look at it, and two gates
give a verdict: accuracy plus thumbs (the naive one), and gate.gate().

Offline results are written to eval_runs in Postgres and read back before the gates run.

    python measure_sycophancy.py [--runs 20]
"""
import argparse
import csv
import json
import statistics

import psycopg

import evals
import harness
from evals import SYSTEM_PROMPT, evaluate
from fake_model import Model, fingerprint
from gate import gate, worse
from users import ab_test, conversations, feedback

PRODUCTION = Model("model-1")
CANDIDATES = [Model(f"cand-bias-{bias:g}", agree_bias=bias) for bias in (0.0, 1.0, 2.0, 3.0)]
SESSIONS = 20            # offline suite: 350 probes x 20 sessions
PANEL_USERS, TURNS = 2000, 5
AB_USERS, AB_PERCENT = 20_000, 2
PROMPT_VERSION = fingerprint(SYSTEM_PROMPT)
RESULTS = harness.HERE / "results"


def one_run(conn: psycopg.Connection, run: int) -> tuple[list[dict], dict]:
    seed, run_id = f"r{run}", f"m5-r{run}"
    sessions = [f"{seed}-s{i}" for i in range(SESSIONS)]
    panel = [req for u in range(PANEL_USERS) for req in conversations(f"{seed}-panel{u}", TURNS)]
    for model in (PRODUCTION, *CANDIDATES):
        evals.save(conn, run_id, model, SYSTEM_PROMPT, evaluate(model, SYSTEM_PROMPT, sessions))
    baseline = evals.load(conn, run_id, PRODUCTION.version, PROMPT_VERSION)

    rows = []
    for model in CANDIDATES:
        offline = evals.load(conn, run_id, model.version, PROMPT_VERSION)
        thumbs = feedback(model, SYSTEM_PROMPT, seed, panel)["thumbs_up"]
        rows.append({
            "run": run, "candidate": model.version, "agree_bias": model.agree_bias,
            "thumbs_up": thumbs.value,
            "known_answer": offline["known_answer"].value,
            "corrects_wrong_premise": offline["wrong_premise"].value,
            "multi_step": offline["multi_step"].value,
            "accuracy_gate_ships": not worse(offline["known_answer"], baseline["known_answer"]),
            "full_gate_ships": gate(offline, baseline).ship,
        })

    winner = max(rows, key=lambda r: r["thumbs_up"])
    picked = next(m for m in CANDIDATES if m.version == winner["candidate"])
    arms = ab_test(PRODUCTION, picked, SYSTEM_PROMPT, seed, AB_USERS, TURNS, AB_PERCENT)
    guardrails = {"corrects_wrong_premise": (arms["candidate"]["corrects_wrong_premise"],
                                             arms["control"]["corrects_wrong_premise"])}
    verdict = gate(evals.load(conn, run_id, picked.version, PROMPT_VERSION), baseline,
                   guardrails=guardrails)
    ab = {
        "run": run, "picked_by_thumbs": picked.version,
        "ab_candidate_turns": arms["candidate"]["thumbs_up"].total,
        "ab_thumbs_control": arms["control"]["thumbs_up"].value,
        "ab_thumbs_candidate": arms["candidate"]["thumbs_up"].value,
        "ab_corrects_control": arms["control"]["corrects_wrong_premise"].value,
        "ab_corrects_candidate": arms["candidate"]["corrects_wrong_premise"].value,
        "accuracy_gate_ships": winner["accuracy_gate_ships"],
        "full_gate_ships": verdict.ship, "reasons": list(verdict.reasons),
    }
    return rows, ab


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    runs = parser.parse_args().runs
    dsn = harness.setup_schema("m5_sycophancy")
    rows, abs_ = [], []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for run in range(runs):
            candidates, ab = one_run(conn, run)
            rows += candidates
            abs_.append(ab)
            print(f"run {run}: thumbs picked {ab['picked_by_thumbs']}; A/B thumbs "
                  f"{ab['ab_thumbs_control']:.1%} -> {ab['ab_thumbs_candidate']:.1%}; "
                  f"gate ships: {ab['full_gate_ships']} {ab['reasons']}")
    harness.drop_schema("m5_sycophancy")

    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / "m5_sycophancy_candidates.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (RESULTS / "m5_sycophancy_ab.json").write_text(json.dumps(abs_, indent=2))

    def stats(values: list[float]) -> dict:
        return {"median": statistics.median(values), "min": min(values), "max": max(values)}

    summary = {"runs": runs, "offline_requests_per_candidate": 350 * SESSIONS,
               "panel_turns_per_candidate": PANEL_USERS * TURNS,
               "ab_users": AB_USERS, "ab_percent": AB_PERCENT, "candidates": {}}
    for model in CANDIDATES:
        cell = [r for r in rows if r["candidate"] == model.version]
        summary["candidates"][model.version] = {
            "agree_bias": model.agree_bias,
            **{key: stats([r[key] for r in cell]) for key in
               ("thumbs_up", "known_answer", "corrects_wrong_premise", "multi_step")},
            "runs_accuracy_gate_ships": sum(r["accuracy_gate_ships"] for r in cell),
            "runs_full_gate_ships": sum(r["full_gate_ships"] for r in cell),
        }
    summary["picked_by_thumbs"] = {m.version: sum(a["picked_by_thumbs"] == m.version for a in abs_)
                                   for m in CANDIDATES}
    summary["ab_test_of_the_pick"] = {
        key: stats([a[key] for a in abs_]) for key in
        ("ab_candidate_turns", "ab_thumbs_control", "ab_thumbs_candidate",
         "ab_corrects_control", "ab_corrects_candidate")}
    summary["runs_accuracy_gate_ships_the_pick"] = sum(a["accuracy_gate_ships"] for a in abs_)
    summary["runs_full_gate_ships_the_pick"] = sum(a["full_gate_ships"] for a in abs_)
    summary["setup"] = harness.MACHINE + "; Python 3.12, NumPy 1.26, psycopg 3.3.6, PostgreSQL 18.6"
    (RESULTS / "m5_sycophancy_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
