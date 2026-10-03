"""M4: 1,000 requests, one clause republished at request 500. How many customers are
quoted the old clause after that?

The model is the stand-in in faithful, word-for-word mode, so every wrong answer here is
the cache's doing. Requests are drawn at random (seeded) from the 150 answerable questions.
At request 500 the refund deadline in REF-03 changes from 90 days to 60. An answer is
STALE when it cites a clause version that is no longer in force.

  naive      key = hash(question)
  versioned  key = hash(question) + the versions of the retrieved clauses

20 runs each. Writes results/m4_cache_runs.csv and results/m4_cache.json.
"""
import csv
import json
import pathlib
import random
import statistics

import psycopg
import redis

from db import DSN, REDIS_URL, publish_clause, reset
from fake_llm import FakeLLM
from questions import KEY, QUESTIONS
from service import cache_key, cache_key_naive, handle

HERE = pathlib.Path(__file__).resolve().parent
RUNS, REQUESTS, BUMP_AT = 20, 1000, 500
CLAUSE = "REF-03"
NEW_BODY = ("To request a refund for an unused ticket, submit the Ticket Refund Application "
            "form within 60 days of the date your ticket was issued.")
KEYS = {"naive": cache_key_naive, "versioned": cache_key}


def one_run(conn: psycopg.Connection, r: redis.Redis, seed: int, key_for) -> dict:
    reset(conn)
    r.flushdb()                                              # Redis DB 6 belongs to this post
    rng = random.Random(seed)
    answerable = [q for q in QUESTIONS if q.kind == "answerable"]
    llm = FakeLLM(seed, KEY, force="faithful", reword_rate=0)
    in_force = {CLAUSE: 1}
    stale = hits = shipped = 0
    for i in range(REQUESTS):
        if i == BUMP_AT:
            in_force[CLAUSE] = publish_clause(conn, CLAUSE, NEW_BODY)
        q = rng.choice(answerable)
        out = handle(conn, r, llm, f"conv-{i}", q.text, key_for=key_for)
        hits += out.cached
        shipped += out.action == "ship"
        stale += any(version != in_force.get(clause_id, 1) for clause_id, version in out.cited)
    return {"stale": stale, "cache_hits": hits, "model_calls": llm.calls, "shipped": shipped}


def main() -> None:
    rows = []
    with psycopg.connect(DSN, autocommit=True) as conn:
        r = redis.Redis.from_url(REDIS_URL)
        for seed in range(1, RUNS + 1):
            for name, key_for in KEYS.items():
                rows.append({"run": seed, "key": name, **one_run(conn, r, seed, key_for)})
        r.flushdb()

    out = HERE / "results"
    with open(out / "m4_cache_runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    summary = {"runs": RUNS, "requests": REQUESTS, "clause_republished_at_request": BUMP_AT,
               "clause": CLAUSE, "keys": {}}
    for name in KEYS:
        mine = [row for row in rows if row["key"] == name]
        summary["keys"][name] = {
            m: {"median": statistics.median(row[m] for row in mine),
                "min": min(row[m] for row in mine), "max": max(row[m] for row in mine)}
            for m in ("stale", "cache_hits", "model_calls", "shipped")}
        summary["keys"][name]["runs_with_a_stale_answer"] = sum(row["stale"] > 0 for row in mine)
    (out / "m4_cache.json").write_text(json.dumps(summary, indent=2) + "\n")
    for name, s in summary["keys"].items():
        print(f"{name:<10} stale {s['stale']['median']:g} ({s['stale']['min']}-{s['stale']['max']}) "
              f"cache hits {s['cache_hits']['median']:g}  model calls {s['model_calls']['median']:g}")


if __name__ == "__main__":
    main()
