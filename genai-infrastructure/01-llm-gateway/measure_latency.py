"""M1: what does the gateway add to a request?

Three paths are compared:
    direct      the load generator calls the mock provider itself
    proxy       through the gateway with budgets and the circuit breaker switched off
    all checks  through the gateway as it ships: Lua budget reservation and settlement,
                circuit breaker, admission, usage record

Two experiments, both with 20-token answers:
    instant     the mock answers at once. 2,000 requests per cell at 1, 16 and 64
                concurrent callers, each caller sending its next request the moment the
                last one returns. At 1 caller this is the gateway's cost per request;
                at 16 and 64 it is what one saturated gateway process can do.
    one second  the mock takes 1,000 ms to its first token, as a model would. 64
                callers, 640 requests per cell: 64 requests a second, which one process
                can keep up with, so this is the cost per request under concurrency.

Non-streamed requests are timed to the last byte, streamed ones to the first token.
Every concurrent caller is its own httpx.AsyncClient with a warm keep-alive connection.
The mock and the gateway each run as one uvicorn process. "Added" is the gateway's
percentile minus the direct path's, round by round.

    python measure_latency.py            # 20 rounds
    python measure_latency.py --rounds 2

Writes results/m1_latency_rounds.csv and results/m1_latency_summary.json.
"""
import argparse
import asyncio
import csv
import json
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

REDIS_URL = "redis://localhost:56379/11"
PROMPT = "Summarize this refund request in one line"
PATHS = {  # name -> environment for the gateway (None: no gateway)
    "direct": None,
    "proxy": {"LLMGW_BUDGET": "off", "LLMGW_BREAKER": "off"},
    "all checks": {"LLMGW_BUDGET": "lua", "LLMGW_BREAKER": "on"},
}
EXPERIMENTS = {  # name -> (the mock's ms to first token, {concurrency: requests per cell})
    "instant": (0, {1: 2000, 16: 2000, 64: 2000}),
    "one second": (1000, {64: 640}),
}


def payload(model: str, stream: bool) -> dict:
    return {"model": model, "max_tokens": 20, "stream": stream,
            "messages": [{"role": "user", "content": PROMPT}]}


async def one(client: httpx.AsyncClient, url: str, headers: dict, body: dict) -> float:
    """Send one request. Returns milliseconds to the first token (streams) or to the
    last byte (everything else)."""
    started = time.perf_counter()
    if not body["stream"]:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        return (time.perf_counter() - started) * 1000
    first = None
    async with client.stream("POST", url, json=body, headers=headers) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            if first is None and '"content": "' in line and '"content": ""' not in line:
                first = (time.perf_counter() - started) * 1000
    return first


async def cell(lanes: list[httpx.AsyncClient], url: str, headers: dict, body: dict,
               total: int) -> dict:
    """`total` requests spread over the lanes, each lane sending one at a time."""
    remaining, timings = total, []

    async def lane(client: httpx.AsyncClient) -> None:
        nonlocal remaining
        while remaining > 0:
            remaining -= 1
            timings.append(await one(client, url, headers, body))

    started = time.perf_counter()
    await asyncio.gather(*(lane(client) for client in lanes))
    wall = time.perf_counter() - started
    return {"p50_ms": round(harness.percentile(timings, 50), 3),
            "p99_ms": round(harness.percentile(timings, 99), 3),
            "mean_ms": round(statistics.fmean(timings), 3),
            "rps": round(total / wall, 1)}


async def main(args: argparse.Namespace, mock: str,
               urls: dict[str, tuple[str, dict, str]]) -> list[dict]:
    lanes = [httpx.AsyncClient(timeout=60, limits=httpx.Limits(max_connections=1))
             for _ in range(64)]
    rows = []
    try:
        for url, headers, model in urls.values():  # open every connection, warm every path
            for stream in (False, True):
                await cell(lanes, url, headers, payload(model, stream), 640)
        for rnd in range(1, args.rounds + 1):
            with measuring("llmgw-m1-latency"):  # one round at a time, so others get a turn
                for experiment, (ttft_ms, cells) in EXPERIMENTS.items():
                    for provider in ("alpha", "beta"):
                        httpx.post(f"{mock}/_control", json={"provider": provider, "mode": "ok",
                                                             "ttft_ms": ttft_ms})
                    for stream in (False, True):
                        for concurrency, total in cells.items():
                            for path, (url, headers, model) in urls.items():
                                result = await cell(lanes[:concurrency], url, headers,
                                                    payload(model, stream), total)
                                row = {"round": rnd, "experiment": experiment, "path": path,
                                       "mode": "stream" if stream else "non-stream",
                                       "concurrency": concurrency, "requests": total, **result}
                                rows.append(row)
                                print(row, flush=True)
    finally:
        for client in lanes:
            await client.aclose()
    return rows


def summarise(rows: list[dict]) -> dict:
    out = {}
    keys = dict.fromkeys((r["experiment"], r["mode"], r["concurrency"]) for r in rows)
    for experiment, mode, concurrency in keys:
        sel = [r for r in rows
               if (r["experiment"], r["mode"], r["concurrency"]) == (experiment, mode, concurrency)]
        direct = {r["round"]: r for r in sel if r["path"] == "direct"}
        for path in PATHS:
            mine = [r for r in sel if r["path"] == path]
            stats = {}
            for field in ("p50_ms", "p99_ms", "rps"):
                values = [r[field] for r in mine]
                stats[field] = {"median": round(statistics.median(values), 3),
                                "min": min(values), "max": max(values)}
            if path != "direct":
                for field in ("p50_ms", "p99_ms"):
                    added = [round(r[field] - direct[r["round"]][field], 3) for r in mine]
                    stats[f"added_{field}"] = {"median": round(statistics.median(added), 3),
                                               "min": min(added), "max": max(added)}
            out[f"{experiment} | {mode} | c={concurrency} | {path}"] = stats
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=20)
    args = ap.parse_args()

    redis.Redis.from_url(REDIS_URL).flushdb()  # DB 11 belongs to this post
    # 64 callers on one virtual key: lift the per-tenant and per-provider caps out of the way.
    caps = {"LLMGW_PER_TENANT": "1000", "LLMGW_PER_PROVIDER": "1000", "LLMGW_REDIS_URL": REDIS_URL}
    with harness.spawn("mock_upstream:app", log_name="m1_mock") as mock:
        env = {**caps, "LLMGW_MOCK_URL": mock}
        with harness.spawn("gateway:app", {**env, **PATHS["proxy"]}, log_name="m1_proxy") as proxy, \
                harness.spawn("gateway:app", {**env, **PATHS["all checks"]}, log_name="m1_full") as full:
            virtual = {"Authorization": "Bearer vk-demo-refunds-acme"}
            urls = {
                "direct": (f"{mock}/alpha/v1/chat/completions",
                           {"Authorization": "Bearer sk-mock-alpha"}, "alpha-large"),
                "proxy": (f"{proxy}/v1/chat/completions", virtual, "support-reply"),
                "all checks": (f"{full}/v1/chat/completions", virtual, "support-reply"),
            }
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            rows = asyncio.run(main(args, mock, urls))

    with open(HERE / "results" / "m1_latency_rounds.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "started": started,
        "setup": {"rounds": args.rounds, "python": platform.python_version(),
                  "experiments": {name: {"mock_ms_to_first_token": ttft, "requests_per_cell": cells}
                                  for name, (ttft, cells) in EXPERIMENTS.items()},
                  "mock": "mock_upstream:app, 1 uvicorn process, 20-token answers",
                  "gateway": "gateway:app, 1 uvicorn process; Redis 8.10.2 in Docker",
                  "client": "asyncio, one httpx.AsyncClient per concurrent caller, keep-alive"},
        "results": summarise(rows),
    }
    (HERE / "results" / "m1_latency_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["results"], indent=2))
