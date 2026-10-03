"""M2: the primary provider goes down. How long does a caller wait for a first token?

Four gateways run side by side against one mock, each with a different defense:
    no breaker                       retries and timeouts only
    first-token timeout              streams give up on a silent provider after 1 s
    breaker                          circuit breaker, no first-token timeout
    breaker + first-token timeout    both, as the gateway ships

Two outages, each watched for 15 seconds:
    hang   the primary accepts the request, answers 200 and then says nothing
    503    the primary answers 503 at once

One streamed request arrives every 250 ms (60 per run and gateway). The fallback is
healthy and takes 50 ms to its first token. The test route gives an attempt 5 s in
total and 1 s to its first token, and the breaker opens for 5 s after 3 failures in a
row. Every request's time to first token is recorded, with its arrival time.

    python measure_failover.py           # 20 runs
    python measure_failover.py --runs 2

Writes results/m2_failover_requests.csv and results/m2_failover_summary.json.
The gateways are started from this file too:  uvicorn measure_failover:app
"""
import argparse
import asyncio
import csv
import json
import os
import pathlib
import platform
import statistics
import sys
import time

import httpx
import redis

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import harness  # noqa: E402
from config import Settings  # noqa: E402
from gateway import create_app  # noqa: E402
from routes import PRICES, Route, Target  # noqa: E402

REDIS_URL = "redis://localhost:56379/11"
PROMPT = "Summarize this refund request in one line"
CONFIGS = {  # name -> (breaker, first-token timeout)
    "no breaker": (False, False),
    "first-token timeout": (False, True),
    "breaker": (True, False),
    "breaker + first-token timeout": (True, True),
}
TIMEOUT_S, FIRST_TOKEN_S, OPEN_S = 5.0, 1.0, 5.0
INTERVAL_S, REQUESTS = 0.25, 60


def make_app():
    """The gateway under test. Each configuration gets its own model names, so the four
    gateways keep separate circuit-breaker state in the one Redis they share."""
    name = os.environ.get("M2_CONFIG", "breaker + first-token timeout")
    breaker, first_token = CONFIGS[name]
    tag = list(CONFIGS).index(name)
    primary = Target("alpha", f"alpha-large-m2-{tag}", TIMEOUT_S, FIRST_TOKEN_S, 64)
    fallback = Target("beta", f"beta-large-m2-{tag}", TIMEOUT_S, FIRST_TOKEN_S, 64)
    PRICES[primary.name], PRICES[fallback.name] = (3.00, 15.00), (2.50, 10.00)
    settings = Settings(redis_url=REDIS_URL, budget="off", breaker=breaker,
                        first_token_timeout=first_token, breaker_open_s=OPEN_S,
                        per_tenant=1000, per_provider=1000)
    return create_app(settings, {"support-reply": Route((primary, fallback))})


app = make_app()


async def first_token(client: httpx.AsyncClient, url: str, arrival: float) -> dict:
    body = {"model": "support-reply", "max_tokens": 20, "stream": True,
            "messages": [{"role": "user", "content": PROMPT}]}
    headers = {"Authorization": "Bearer vk-demo-refunds-acme"}
    started, ttft = time.perf_counter(), None
    async with client.stream("POST", f"{url}/v1/chat/completions", json=body,
                             headers=headers) as resp:
        target = resp.headers.get("x-llmgw-target", "")
        attempts = int(resp.headers.get("x-llmgw-attempts", "0"))
        async for line in resp.aiter_lines():
            if ttft is None and '"content": "' in line and '"content": ""' not in line:
                ttft = time.perf_counter() - started
    if ttft is None:  # no token at all: count the whole wait
        ttft = time.perf_counter() - started
    return {"arrival_s": round(arrival, 2), "ttft_s": round(ttft, 4), "status": resp.status_code,
            "served_by": target.split("/")[0], "upstream_calls": attempts}


async def run(urls: dict[str, str]) -> list[dict]:
    """One 15-second outage: the same arrivals go to every gateway at the same time."""
    limits = httpx.Limits(max_connections=None)
    async with httpx.AsyncClient(timeout=60, limits=limits) as client:
        tasks = []
        started = time.perf_counter()
        for k in range(REQUESTS):
            await asyncio.sleep(max(0.0, started + k * INTERVAL_S - time.perf_counter()))
            for config, url in urls.items():
                tasks.append((config, asyncio.ensure_future(
                    first_token(client, url, time.perf_counter() - started))))
        return [{"config": config, **await task} for config, task in tasks]


def summarise(rows: list[dict]) -> dict:
    out = {}
    for scenario in dict.fromkeys(r["scenario"] for r in rows):
        for config in CONFIGS:
            sel = [r for r in rows if (r["scenario"], r["config"]) == (scenario, config)]
            runs = sorted({r["run"] for r in sel})
            per_run = []
            for n in runs:
                waits = [r["ttft_s"] for r in sel if r["run"] == n]
                per_run.append({
                    "p50": harness.percentile(waits, 50), "p95": harness.percentile(waits, 95),
                    "max": max(waits), "mean": statistics.fmean(waits),
                    "waited_over_500ms": sum(w > 0.5 for w in waits),
                    "total_wait": sum(waits)})
            stats = {"runs": len(runs), "requests_per_run": len(sel) // len(runs),
                     "served_by_fallback": sum(r["served_by"] == "beta" for r in sel),
                     "not_200": sum(r["status"] != 200 for r in sel)}
            for field in per_run[0]:
                values = [run[field] for run in per_run]
                stats[field] = {"median": round(statistics.median(values), 4),
                                "min": round(min(values), 4), "max": round(max(values), 4)}
            out[f"{scenario} | {config}"] = stats
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    args = ap.parse_args()

    rds = redis.Redis.from_url(REDIS_URL)
    rows = []
    with harness.spawn("mock_upstream:app", log_name="m2_mock") as mock:
        env = {"LLMGW_MOCK_URL": mock}
        servers = [harness.spawn("measure_failover:app", {**env, "M2_CONFIG": name},
                                 log_name=f"m2_gateway_{i}") for i, name in enumerate(CONFIGS)]
        urls = {name: server.__enter__() for name, server in zip(CONFIGS, servers)}
        try:
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            asyncio.run(run(urls))  # an unrecorded pass with healthy providers: warm everything
            for n in range(1, args.runs + 1):
                with measuring("llmgw-m2-failover"):  # one run at a time, both outages
                    for scenario in ("hang", "503"):
                        rds.flushdb()  # DB 11 belongs to this post: every outage starts closed
                        httpx.post(f"{mock}/_reset")
                        httpx.post(f"{mock}/_control", json={"provider": "beta", "mode": "ok",
                                                             "ttft_ms": 50})
                        down = ({"mode": "hang"} if scenario == "hang"
                                else {"mode": "status", "error": "overloaded_503"})
                        httpx.post(f"{mock}/_control", json={"provider": "alpha", **down})
                        results = asyncio.run(run(urls))
                        for row in results:
                            rows.append({"run": n, "scenario": scenario, **row})
                        print(n, scenario, {c: round(statistics.fmean(
                            r["ttft_s"] for r in results if r["config"] == c), 3)
                            for c in CONFIGS}, flush=True)
        finally:
            for server in servers:
                server.__exit__(None, None, None)

    with open(HERE / "results" / "m2_failover_requests.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "started": started,
        "setup": {"runs": args.runs, "requests_per_run": REQUESTS, "interval_s": INTERVAL_S,
                  "attempt_timeout_s": TIMEOUT_S, "first_token_timeout_s": FIRST_TOKEN_S,
                  "breaker": f"opens for {OPEN_S:g} s after 3 failures in a row",
                  "fallback_ms_to_first_token": 50, "python": platform.python_version(),
                  "gateway": "4 uvicorn processes, one per configuration, attempts=3, "
                             "backoff base 0.5 s with full jitter"},
        "results": summarise(rows),
    }
    (HERE / "results" / "m2_failover_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["results"], indent=2))
