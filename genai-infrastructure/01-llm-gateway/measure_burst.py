"""M1b: what does a burst cost? 64 streamed requests land in the same instant.

measure_latency.py lets every caller send its next request the moment the last one
returns, so after the first wave the callers drift apart and the gateway is never asked
to start 64 requests at once again. This script keeps them together. The mock takes
1,000 ms to its first token; each wave, all 64 callers fire at the same moment, and the
next wave starts when the last stream of this one has ended.

Before the first wave of every run the script waits 6 seconds. httpx closes idle
keep-alive connections after 5, so the gateway meets wave 1 with no open connection to
the provider (a cold pool), and waves 2 to 6 with 64 warm ones. The callers' own
connections are kept open throughout, on both paths.

    direct      the callers hit the mock provider themselves
    all checks  through the gateway as it ships

    python measure_burst.py              # 10 runs of 6 waves per path

Writes results/m1b_burst_waves.csv and results/m1b_burst_summary.json.
measure_pool_burst.py repeats the burst with httpx alone, to show which part of the
cost belongs to the HTTP client's connection pool.
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
from measure_latency import REDIS_URL, one, payload  # noqa: E402

CALLERS, WAVES, IDLE_S, TTFT_MS = 64, 6, 6.0, 1000


async def run(urls: dict[str, tuple[str, dict, str]], n: int) -> list[dict]:
    """One run: per path, go idle, then fire WAVES waves of CALLERS requests each."""
    limits = httpx.Limits(max_connections=1, keepalive_expiry=600)
    lanes = [httpx.AsyncClient(timeout=60, limits=limits) for _ in range(CALLERS)]
    rows = []
    try:
        for path, (url, headers, model) in urls.items():
            body = payload(model, stream=True)
            await asyncio.gather(*(one(lane, url, headers, body) for lane in lanes))  # connect
            await asyncio.sleep(IDLE_S)
            for wave in range(1, WAVES + 1):
                waits = await asyncio.gather(*(one(lane, url, headers, body) for lane in lanes))
                rows.append({"run": n, "path": path, "wave": wave,
                             "p50_ms": round(harness.percentile(waits, 50), 1),
                             "max_ms": round(max(waits), 1)})
                print(rows[-1], flush=True)
    finally:
        for lane in lanes:
            await lane.aclose()
    return rows


def summarise(rows: list[dict]) -> dict:
    out = {}
    for label, waves in (("wave 1 (cold pool)", [1]), ("waves 2-6 (warm pool)", range(2, WAVES + 1))):
        stats = {}
        for field in ("p50_ms", "max_ms"):
            added = []
            for r in rows:
                if r["path"] == "all checks" and r["wave"] in waves:
                    direct = next(d for d in rows if d["path"] == "direct"
                                  and (d["run"], d["wave"]) == (r["run"], r["wave"]))
                    added.append(round(r[field] - direct[field], 1))
            stats[f"added_{field}"] = {"median": statistics.median(added), "min": min(added),
                                       "max": max(added), "waves": len(added)}
        out[label] = stats
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=10)
    args = ap.parse_args()

    redis.Redis.from_url(REDIS_URL).flushdb()  # DB 11 belongs to this post
    caps = {"LLMGW_PER_TENANT": "1000", "LLMGW_PER_PROVIDER": "1000", "LLMGW_REDIS_URL": REDIS_URL}
    rows = []
    with harness.spawn("mock_upstream:app", {"MOCK_TTFT_MS": str(TTFT_MS)},
                       log_name="m1b_mock") as mock:
        with harness.spawn("gateway:app", {**caps, "LLMGW_MOCK_URL": mock},
                           log_name="m1b_gateway") as gateway:
            urls = {
                "direct": (f"{mock}/alpha/v1/chat/completions",
                           {"Authorization": "Bearer sk-mock-alpha"}, "alpha-large"),
                "all checks": (f"{gateway}/v1/chat/completions",
                               {"Authorization": "Bearer vk-demo-refunds-acme"}, "support-reply"),
            }
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            for n in range(1, args.runs + 1):
                with measuring("llmgw-m1b-burst", poll=0.002):  # one run at a time, so others get a turn
                    rows += asyncio.run(run(urls, n))

    with open(HERE / "results" / "m1b_burst_waves.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "started": started,
        "setup": {"runs": args.runs, "waves_per_run": WAVES, "callers": CALLERS,
                  "idle_before_wave_1_s": IDLE_S, "mock_ms_to_first_token": TTFT_MS,
                  "python": platform.python_version(),
                  "gateway": "gateway:app, 1 uvicorn process; Redis 8.10.2 in Docker",
                  "client": "asyncio, one httpx.AsyncClient per caller, kept alive"},
        "results": summarise(rows),
    }
    (HERE / "results" / "m1b_burst_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["results"], indent=2))
