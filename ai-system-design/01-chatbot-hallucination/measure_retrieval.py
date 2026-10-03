"""Why retrieve() orders by word coverage and uses ts_rank_cd only to break ties.

For the 200 questions that have a right clause: how often is that clause ranked first, and
how often is it in the top 5, with each ordering? And how well does each score separate
questions that have a clause from questions that don't?

Writes results/m0_retrieval.json.
"""
import json
import pathlib
import statistics

import psycopg

from bot import K, RETRIEVE
from db import DSN, reset
from questions import QUESTIONS

HERE = pathlib.Path(__file__).resolve().parent
# The same query as bot.RETRIEVE with ts_rank_cd as the score and the only ordering.
RANK_CD_ONLY = """
WITH q AS (
    SELECT CAST(replace(plainto_tsquery('english', %(question)s)::text, '&', '|') AS tsquery)
               AS any_word
)
SELECT clause_id, version, policy_type, body, ts_rank_cd(tsv, any_word) AS score
FROM policy_clause, q
WHERE effective @> now() AND tsv @@ any_word
ORDER BY score DESC, clause_id
LIMIT %(k)s
"""


def evaluate(conn: psycopg.Connection, sql: str) -> dict:
    first = top_k = 0
    best = {"has_clause": [], "no_clause": []}
    for q in QUESTIONS:
        rows = conn.execute(sql, {"question": q.text, "k": K}).fetchall()
        ids = [row[0] for row in rows]
        best["has_clause" if q.gold else "no_clause"].append(rows[0][4] if rows else 0.0)
        if q.gold:
            first += bool(ids) and ids[0] == q.gold
            top_k += q.gold in ids
    return {"right_clause_first": first, f"right_clause_in_top_{K}": top_k,
            "questions_with_a_clause": sum(1 for q in QUESTIONS if q.gold),
            "median_best_score": {k: round(statistics.median(v), 3) for k, v in best.items()}}


def main() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        reset(conn)
        result = {"coverage_then_ts_rank_cd": evaluate(conn, RETRIEVE),
                  "ts_rank_cd_only": evaluate(conn, RANK_CD_ONLY)}
    (HERE / "results" / "m0_retrieval.json").write_text(json.dumps(result, indent=2) + "\n")
    for name, r in result.items():
        print(name, r)


if __name__ == "__main__":
    main()
