"""M3: what the gates cost in time, one call at a time from Python.

- route():        the keyword router, pure Python
- retrieve():     one round trip to Postgres in Docker over localhost
- tier1_check():  pure Python, on every draft
- judge():        the tier-2 stand-in, pure Python (a real LLM evaluator costs a model call)

20 rounds over the 300 questions (6,000 calls of route and retrieve; the checks run on
every question that retrieved a clause). Timings are taken inside the repo's measure lock.
Writes results/m3_latency_samples.csv and results/m3_latency.json.
"""
import csv
import json
import pathlib
import sys
import time

import numpy as np
import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

from bot import retrieve, route  # noqa: E402
from checks import judge, tier1_check  # noqa: E402
from db import DSN, reset  # noqa: E402
from fake_llm import FakeLLM  # noqa: E402
from questions import KEY, QUESTIONS  # noqa: E402

ROUNDS = 20


def timed(fn, *args):
    t0 = time.perf_counter_ns()
    result = fn(*args)
    return result, (time.perf_counter_ns() - t0) / 1000     # microseconds


def main() -> None:
    samples = []
    with psycopg.connect(DSN, autocommit=True) as conn, measuring("chatbot-gate-latency"):
        reset(conn)
        for q in QUESTIONS:                                   # warm up the connection and caches
            retrieve(conn, q.text)
        for rnd in range(1, ROUNDS + 1):
            llm = FakeLLM(rnd, KEY)
            for q in QUESTIONS:
                intent, t_route = timed(route, q.text)
                clauses, t_retrieve = timed(retrieve, conn, q.text)
                samples += [(rnd, "route", t_route), (rnd, "retrieve", t_retrieve)]
                if clauses:
                    draft = llm.draft(q.text, clauses)
                    _, t_tier1 = timed(tier1_check, draft, clauses, intent)
                    _, t_judge = timed(judge, draft, clauses, intent)
                    samples += [(rnd, "tier1_check", t_tier1), (rnd, "judge", t_judge)]

    out = HERE / "results"
    with open(out / "m3_latency_samples.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["round", "step", "microseconds"])
        w.writerows((r, s, f"{us:.1f}") for r, s, us in samples)

    summary = {"rounds": ROUNDS, "questions": len(QUESTIONS), "unit": "microseconds", "steps": {}}
    for step in ("route", "retrieve", "tier1_check", "judge"):
        us = np.array([t for _, s, t in samples if s == step])
        summary["steps"][step] = {"calls": int(us.size),
                                  "p50": round(float(np.percentile(us, 50)), 1),
                                  "p95": round(float(np.percentile(us, 95)), 1),
                                  "p99": round(float(np.percentile(us, 99)), 1),
                                  "max": round(float(us.max()), 1)}
    (out / "m3_latency.json").write_text(json.dumps(summary, indent=2) + "\n")
    for step, s in summary["steps"].items():
        print(f"{step:<12} {s['calls']:>5} calls  p50 {s['p50']:>8.1f} us  p95 {s['p95']:>8.1f} us  "
              f"p99 {s['p99']:>8.1f} us")


if __name__ == "__main__":
    main()
