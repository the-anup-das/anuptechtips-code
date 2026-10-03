"""M1: 300 questions through four configurations of the bot, 20 seeds.

For every seed the stand-in model (fake_llm.FakeLLM) writes the same draft for the same
question in every configuration, so the only thing that changes is which gates look at it:

  none        answer_naive(): retrieve, draft, send
  scope       + scope and threshold: legal questions and weak retrieval go to a human
  scope+check + the answer check: tier 1 on every draft, the judge stand-in on tier-1 failures
  all gates   + the commitment guard: promises are answered by eligibility() or handed off

plus one ablation, "scope+check, no type check", which shows what the policy-type check adds.

A shipped answer is UNSAFE unless it is the right clause, faithfully (or a backend decision).
A FALSE HAND-OFF is a hand-off of a question the stand-in had answered correctly.

Writes results/m1_gates_answers.csv (one row per seed, configuration and question),
results/m1_gates_runs.csv (one row per seed and configuration) and
results/m1_gates_summary.json.
"""
import contextlib
import csv
import json
import pathlib
import statistics
from collections import Counter

import psycopg

import bot
from bot import ALL_GATES, TAU, answer, answer_naive, retrieve, route
from checks import JUDGE_THETA, THETA, cited_clauses
from corpus import CLAUSES
from db import DSN, reset
from fake_llm import RATES, REWORD_RATE, FakeLLM
from questions import KEY, QUESTIONS

HERE = pathlib.Path(__file__).resolve().parent
SEEDS = range(1, 21)
SCOPE, SCOPE_CHECK = frozenset({"scope"}), frozenset({"scope", "check"})
CONFIGS = {"none": None, "scope": SCOPE, "scope+check, no type check": SCOPE_CHECK,
           "scope+check": SCOPE_CHECK, "all gates": ALL_GATES}
SHAPES = ("blend", "wrong_clause", "gap_fill", "agree", "retrieval_miss")
KINDS = ("answerable", "no_policy", "legal", "leading")
CAUSES = ("out_of_scope", "no_policy", "policy_type", "facts_or_wording", "commitment")


@contextlib.contextmanager
def no_type_check():
    """Ablation: run gate 2 as if the router always agreed with the clause the draft cites."""
    def trusting(check):
        def wrapped(draft, clauses, intent):
            cited = cited_clauses(draft, clauses)
            return check(draft, clauses, cited[0].policy_type if cited else intent)
        return wrapped
    saved = bot.tier1_check, bot.judge
    bot.tier1_check, bot.judge = trusting(saved[0]), trusting(saved[1])
    try:
        yield
    finally:
        bot.tier1_check, bot.judge = saved


def is_right(q, draft) -> bool:
    """The stand-in answered from the right clause without changing it."""
    return bool(q.gold and draft.mode == "faithful" and draft.cited
                and draft.cited[0][0] == q.gold)


def cause(out) -> str:
    """Which gate, and which check inside gate 2, sent this to a human."""
    if out.stage != "answer_check":
        return out.stage
    return "policy_type" if "policy_type" in out.problems else "facts_or_wording"


def run(conn: psycopg.Connection, seeds=SEEDS, tau: float = TAU, configs=CONFIGS):
    """Yield one dict per seed, configuration and question."""
    clauses = {q.text: retrieve(conn, q.text) for q in QUESTIONS}   # same for every seed
    for seed in seeds:
        reference = FakeLLM(seed, KEY)
        drafts = {q.text: reference.draft(q.text, clauses[q.text]) for q in QUESTIONS}
        for name, gates in configs.items():
            llm = FakeLLM(seed, KEY)
            with no_type_check() if "no type check" in name else contextlib.nullcontext():
                outs = [answer_naive(llm, q.text, clauses[q.text]) if gates is None
                        else answer(llm, q.text, clauses[q.text], q.ticket, tau, gates)
                        for q in QUESTIONS]
            for i, (q, out) in enumerate(zip(QUESTIONS, outs)):
                draft = drafts[q.text]
                assert out.draft in (None, draft)       # the gates never change the draft
                right, shipped = is_right(q, draft), out.action == "ship"
                unsafe = shipped and out.stage != "backend" and not right
                yield {
                    "seed": seed, "config": name, "question": i, "kind": q.kind,
                    "mode": draft.mode, "action": out.action, "stage": out.stage,
                    "problems": "+".join(out.problems), "drafted": int(out.draft is not None),
                    "tier2": int(out.tier2), "right_draft": int(right), "unsafe": int(unsafe),
                    "false_handoff": int(not shipped and right),
                    "shape": "" if right else
                             "retrieval_miss" if draft.mode == "faithful" else draft.mode,
                    "cause": "" if shipped else cause(out),
                }


def count(rows: list[dict]) -> dict[tuple, Counter]:
    """Totals per (seed, configuration)."""
    runs: dict[tuple, Counter] = {}
    for r in rows:
        c = runs.setdefault((r["seed"], r["config"]), Counter())
        handoff = r["action"] == "handoff"
        c["unsafe"] += r["unsafe"]
        c["false_handoffs"] += r["false_handoff"]
        c["handoffs"] += handoff
        c["backend_answers"] += r["stage"] == "backend"
        c["drafted"] += r["drafted"]
        c["tier2"] += r["tier2"]
        c["right_drafts_checked"] += bool(r["right_draft"] and r["drafted"]
                                          and r["stage"] in ("shipped", "answer_check"))
        c["right_drafts_to_tier2"] += bool(r["right_draft"] and r["tier2"])
        if r["unsafe"]:
            c[f"unsafe_{r['shape']}"] += 1
            c[f"unsafe_kind_{r['kind']}"] += 1
        if r["false_handoff"]:
            c[f"false_handoff_{r['cause']}"] += 1
        if r["mode"] == "wrong_clause" and r["stage"] in ("shipped", "answer_check"):
            c["wrong_clause_checked"] += 1
            c["wrong_clause_stopped_by_type_only"] += r["problems"] == "policy_type"
    return runs


METRICS = (["unsafe", "false_handoffs", "handoffs", "backend_answers", "drafted", "tier2",
            "right_drafts_checked", "right_drafts_to_tier2", "wrong_clause_checked",
            "wrong_clause_stopped_by_type_only"]
           + [f"unsafe_{s}" for s in SHAPES] + [f"unsafe_kind_{k}" for k in KINDS]
           + [f"false_handoff_{c}" for c in CAUSES])


def stats(values: list[float]) -> dict:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def router_report() -> dict:
    """How the keyword router does on the question set (it is the same for every seed)."""
    types = {c.clause_id: c.policy_type for c in CLAUSES}
    out = Counter()
    for q in QUESTIONS:
        intent = route(q.text)
        if q.gold:
            out["with_clause_" + ("right" if intent == types[q.gold] else
                                  "no_intent" if intent is None else
                                  "sent_to_legal" if intent == "legal" else "wrong_type")] += 1
        else:
            out[f"{q.kind}_" + ("sent_to_legal" if intent == "legal" else
                                "no_intent" if intent is None else "typed")] += 1
    return dict(sorted(out.items()))


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        reset(conn)
        rows = list(run(conn))

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m1_gates_answers.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    runs = count(rows)
    with open(out / "m1_gates_runs.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seed", "config", *METRICS])
        for seed in SEEDS:
            for config in CONFIGS:
                w.writerow([seed, config, *(runs[(seed, config)][m] for m in METRICS)])

    summary = {"questions": len(QUESTIONS), "seeds": len(SEEDS),
               "kinds": dict(Counter(q.kind for q in QUESTIONS)),
               "settings": {"tau": TAU, "theta": THETA, "judge_theta": JUDGE_THETA,
                            "k": bot.K, "rates": RATES, "reword_rate": REWORD_RATE},
               "router": router_report(), "configs": {}}
    for config in CONFIGS:
        per_seed = [runs[(seed, config)] for seed in SEEDS]
        s = {m: stats([c[m] for c in per_seed]) for m in METRICS}
        s["handoff_rate_pct"] = stats([round(100 * c["handoffs"] / len(QUESTIONS), 1)
                                       for c in per_seed])
        s["tier2_share_of_drafts_pct"] = stats(
            [round(100 * c["tier2"] / c["drafted"], 1) for c in per_seed])
        s["tier2_share_of_right_drafts_pct"] = stats(
            [round(100 * c["right_drafts_to_tier2"] / c["right_drafts_checked"], 1)
             if c["right_drafts_checked"] else 0.0 for c in per_seed])
        summary["configs"][config] = s
    (out / "m1_gates_summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    def cell(s, m):
        return f"{s[m]['median']:g} ({s[m]['min']:g}-{s[m]['max']:g})"
    print(f"{'config':<20} {'unsafe':>14} {'false hand-offs':>16} {'hand-offs':>14} "
          f"{'drafted':>9} {'to tier 2':>14} {'backend':>9}")
    for config, s in summary["configs"].items():
        print(f"{config:<20} {cell(s, 'unsafe'):>14} {cell(s, 'false_handoffs'):>16} "
              f"{cell(s, 'handoffs'):>14} {s['drafted']['median']:>9g} {cell(s, 'tier2'):>14} "
              f"{cell(s, 'backend_answers'):>9}")
    for config, s in summary["configs"].items():
        print(f"{config}: unsafe by shape", {k: s[f'unsafe_{k}']['median'] for k in SHAPES},
              "| false hand-offs by cause",
              {k: s[f'false_handoff_{k}']['median'] for k in CAUSES})
    print("router:", summary["router"])


if __name__ == "__main__":
    main()
