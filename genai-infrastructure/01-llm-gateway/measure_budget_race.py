"""M4: 200 requests arrive at once for a tenant whose budget covers 10 of them.

Two gateways, the same request, the same budget:
    check, then spend      naive_budget.py: read the counter, call the model, add the cost
    reserve, then settle   budget.py: one Lua script debits the worst case before the call

Every request asks for 20 tokens on the 'summarize' route and gets 20, so each one
costs exactly its worst case (321 micro-USD), and the tenant's limit is 10 times that.
The mock takes 2 seconds per call, so all 200 are in flight together, as they would be
against a real model. Each request has its own HTTP connection.

    python measure_budget_race.py        # 20 runs of each

Writes results/m4_budget_race_runs.csv and results/m4_budget_race_summary.json.
"""
import argparse
import asyncio
import csv
import json
import pathlib
import platform
import statistics
import time

import httpx
import redis

import harness
from budget import limit_key, spent_key
from routes import ROUTES, cost_micro_usd

HERE = pathlib.Path(__file__).resolve().parent
REDIS_URL = "redis://localhost:56379/11"
PROMPT = "Summarize this refund request in one line"  # 7 tokens for the mock
N, FITS, MODEL_CALL_MS = 200, 10, 2000
WORST = cost_micro_usd(ROUTES["summarize"].chain[0], 7, 20)
MODES = {"check, then spend": "naive", "reserve, then settle": "lua"}


async def burst(lanes: list[httpx.AsyncClient], url: str) -> dict:
    body = {"model": "summarize", "max_tokens": 20,
            "messages": [{"role": "user", "content": PROMPT}]}
    headers = {"Authorization": "Bearer vk-demo-refunds-acme"}
    responses = await asyncio.gather(*(lane.post(f"{url}/v1/chat/completions", json=body,
                                                 headers=headers) for lane in lanes))
    codes = [resp.status_code for resp in responses]
    return {"served_200": codes.count(200), "refused_429": codes.count(429),
            "other": len(codes) - codes.count(200) - codes.count(429)}


async def main(args: argparse.Namespace, urls: dict[str, str]) -> list[dict]:
    rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    lanes = [httpx.AsyncClient(timeout=60, limits=httpx.Limits(max_connections=1))
             for _ in range(N)]
    rows = []
    try:
        for url in urls.values():  # open every connection before the first measured burst
            await asyncio.gather(*(lane.get(f"{url}/openapi.json") for lane in lanes))
        for run in range(1, args.runs + 1):
            for name, url in urls.items():
                rds.flushdb()  # DB 11 belongs to this post: a fresh month for the tenant
                rds.set(limit_key("demo", "tenant:acme"), FITS * WORST)
                result = await burst(lanes, url)
                spent = int(rds.get(spent_key("demo", "tenant:acme")) or 0)
                row = {"run": run, "budget": name, **result, "limit_micro_usd": FITS * WORST,
                       "spent_micro_usd": spent, "times_the_budget": round(spent / (FITS * WORST), 2)}
                rows.append(row)
                print(row, flush=True)
    finally:
        for lane in lanes:
            await lane.aclose()
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    args = ap.parse_args()

    started = time.strftime("%Y-%m-%d %H:%M:%S")
    with harness.spawn("mock_upstream:app", {"MOCK_TTFT_MS": str(MODEL_CALL_MS)},
                       log_name="m4_mock") as mock:
        env = {"LLMGW_MOCK_URL": mock, "LLMGW_REDIS_URL": REDIS_URL, "LLMGW_BREAKER": "off",
               "LLMGW_PER_TENANT": "1000", "LLMGW_PER_PROVIDER": "1000"}
        servers = {name: harness.spawn("gateway:app", {**env, "LLMGW_BUDGET": mode},
                                       log_name=f"m4_gateway_{mode}")
                   for name, mode in MODES.items()}
        urls = {name: server.__enter__() for name, server in servers.items()}
        try:
            rows = asyncio.run(main(args, urls))
        finally:
            for server in servers.values():
                server.__exit__(None, None, None)

    with open(HERE / "results" / "m4_budget_race_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    results = {}
    for name in MODES:
        sel = [r for r in rows if r["budget"] == name]
        results[name] = {"runs": len(sel)}
        for field in ("served_200", "refused_429", "other", "spent_micro_usd", "times_the_budget"):
            values = [r[field] for r in sel]
            results[name][field] = {"median": statistics.median(values), "min": min(values),
                                    "max": max(values)}
    summary = {"started": started,
               "setup": {"runs": args.runs, "requests_at_once": N, "budget_fits": FITS,
                         "worst_case_micro_usd": WORST, "limit_micro_usd": FITS * WORST,
                         "model_call_ms": MODEL_CALL_MS, "python": platform.python_version(),
                         "gateway": "1 uvicorn process per budget mode; Redis 8.10.2 in Docker"},
               "results": results}
    (HERE / "results" / "m4_budget_race_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(results, indent=2))
