"""M3: one click, three layers that each retry. How many calls reach a provider that
is down?

The provider (the mock) answers every call with a 503. The layers are real: an
application loop that tries 3 times, the openai Python SDK with its own retry settings,
and the gateway with its 3 attempts per target, on a route with a single target.

    stacked defaults    app 3 tries, SDK default (2 retries), gateway 3 attempts
    sdk retries off     app 3 tries, SDK max_retries=0,       gateway 3 attempts
    gateway only        app 1 try,   SDK max_retries=0,       gateway 3 attempts
    retry hint          app 3 tries, SDK default,             gateway 3 attempts, and the
                        gateway answers with "x-should-retry: false"

The circuit breaker is off in all four, so that only the retry layers are counted.
The counts come from the mock's own call counter.

    python measure_retry_stacking.py     # 20 runs of each

Writes results/m3_retry_stacking_runs.csv and results/m3_retry_stacking_summary.json.
"""
import argparse
import csv
import json
import pathlib
import platform
import statistics
import sys
import time

import httpx
import openai
import redis

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import harness  # noqa: E402

REDIS_URL = "redis://localhost:56379/11"
PROMPT = "Summarize this refund request in one line"
VARIANTS = {  # name -> (app tries, SDK options, gateway sends the retry hint)
    "stacked defaults": (3, {}, False),
    "sdk retries off": (3, {"max_retries": 0}, False),
    "gateway only": (1, {"max_retries": 0}, False),
    "retry hint": (3, {}, True),
}


def one_click(gateway_url: str, app_tries: int, sdk_options: dict) -> int:
    """What application code does around an SDK call. Returns the tries it made."""
    client = openai.OpenAI(base_url=f"{gateway_url}/v1", api_key="vk-demo-refunds-acme",
                           **sdk_options)
    tries = 0
    for _ in range(app_tries):
        tries += 1
        try:
            client.chat.completions.create(model="summarize",
                                           messages=[{"role": "user", "content": PROMPT}])
            break
        except openai.APIStatusError:
            continue
    client.close()
    return tries


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    args = ap.parse_args()

    redis.Redis.from_url(REDIS_URL).flushdb()  # DB 11 belongs to this post
    rows = []
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    with harness.spawn("mock_upstream:app", log_name="m3_mock") as mock:
        env = {"LLMGW_MOCK_URL": mock, "LLMGW_REDIS_URL": REDIS_URL, "LLMGW_BREAKER": "off"}
        with harness.spawn("gateway:app", {**env, "LLMGW_RETRY_HINT": "off"},
                           log_name="m3_gateway_plain") as plain, \
                harness.spawn("gateway:app", {**env, "LLMGW_RETRY_HINT": "on"},
                              log_name="m3_gateway_hint") as hinting:
            for run in range(1, args.runs + 1):
                with measuring("llmgw-m3-retry-stacking", poll=0.002):  # the seconds are a timing
                    for name, (app_tries, sdk_options, hint) in VARIANTS.items():
                        httpx.post(f"{mock}/_reset")
                        httpx.post(f"{mock}/_control", json={"provider": "alpha", "mode": "status",
                                                             "error": "overloaded_503"})
                        began = time.perf_counter()
                        tries = one_click(hinting if hint else plain, app_tries, sdk_options)
                        seconds = time.perf_counter() - began
                        upstream = httpx.get(f"{mock}/_stats").json()["alpha"]["calls"]
                        row = {"run": run, "variant": name, "app_tries": tries,
                               "sdk_max_retries": sdk_options.get("max_retries", "default (2)"),
                               "upstream_calls": upstream, "seconds": round(seconds, 2)}
                        rows.append(row)
                        print(row, flush=True)

    with open(HERE / "results" / "m3_retry_stacking_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    results = {}
    for name in VARIANTS:
        calls = [r["upstream_calls"] for r in rows if r["variant"] == name]
        seconds = [r["seconds"] for r in rows if r["variant"] == name]
        results[name] = {"runs": len(calls),
                         "upstream_calls": {"median": statistics.median(calls), "min": min(calls),
                                            "max": max(calls)},
                         "seconds": {"median": statistics.median(seconds), "min": min(seconds),
                                     "max": max(seconds)}}
    summary = {"started": started,
               "setup": {"runs": args.runs, "python": platform.python_version(),
                         "openai_sdk": openai.__version__,
                         "sdk_default_max_retries": openai.DEFAULT_MAX_RETRIES,
                         "gateway": "attempts=3 per target, backoff base 0.5 s, breaker off, "
                                    "route 'summarize' (one target)"},
               "results": results}
    (HERE / "results" / "m3_retry_stacking_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(results, indent=2))
