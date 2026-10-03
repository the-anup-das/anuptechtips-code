"""The offline suite: every probe, in several sessions, straight at one model + prompt.
Results go to Postgres, where the gate and the ablation read them."""
from collections import Counter
from collections.abc import Sequence

import psycopg

from fake_model import Model, Probe, Request, Serving, fingerprint, respond
from gate import Rate
from probes import PROBES, score

SYSTEM_PROMPT = [
    "You are a coding assistant.",
    "Prefer small, reviewable changes.",
    "Run the tests before you report success.",
    "Never invent file paths.",
    "Ask before deleting files.",
]
CAP_LINE = "Keep text between tool calls to 25 words or fewer."   # the injected prompt regression


def evaluate(model: Model, system_prompt: Sequence[str], sessions: Sequence[str],
             serving: Serving = Serving(), probes: Sequence[Probe] = PROBES,
             batch_size: int = 8) -> dict[str, Rate]:
    """Pass rate per suite over len(probes) x len(sessions) requests."""
    requests = [Request(probe, session) for session in sessions for probe in probes]
    passed, total = Counter(), Counter()
    for i in range(0, len(requests), batch_size):
        batch = requests[i:i + batch_size]
        for req, tokens in zip(batch, respond(model, serving, system_prompt, batch)):
            total[req.probe.suite] += 1
            passed[req.probe.suite] += score(req.probe, tokens)
    return {suite: Rate(passed[suite], total[suite]) for suite in total}


def save(conn: psycopg.Connection, run_id: str, model: Model, system_prompt: Sequence[str],
         scores: dict[str, Rate]) -> None:
    for suite, rate in scores.items():
        conn.execute(
            "INSERT INTO eval_runs (run_id, model_version, prompt_version, suite, passed, total) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (run_id, model_version, prompt_version, suite) "
            "DO UPDATE SET passed = EXCLUDED.passed, total = EXCLUDED.total",
            (run_id, model.version, fingerprint(system_prompt), suite, rate.passed, rate.total))


def load(conn: psycopg.Connection, run_id: str, model_version: str,
         prompt_version: str) -> dict[str, Rate]:
    rows = conn.execute(
        "SELECT suite, passed, total FROM eval_runs "
        "WHERE run_id = %s AND model_version = %s AND prompt_version = %s",
        (run_id, model_version, prompt_version)).fetchall()
    return {suite: Rate(passed, total) for suite, passed, total in rows}
