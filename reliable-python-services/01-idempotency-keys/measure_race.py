"""M1: fire N same-key POSTs at once and count what happened.

Starts race_app.py under uvicorn, then for every run uses a fresh client id and
Idempotency-Key, sends N identical POSTs concurrently (asyncio + httpx), and counts the
rows that landed in `charges` plus every status code.

Each concurrent request gets its own httpx.AsyncClient ("lane") with a warm keep-alive
connection. On the test machine one shared AsyncClient with 100 requests in flight took
about 2.1 s to deliver 100 requests that each sleep 50 ms on the server, so the burst
wasn't a burst; with one client per lane the same 100 requests finish in about 100-140 ms.

    python measure_race.py                       # 20 runs x N=50,100 x 3 strategies
    python measure_race.py --threads 40 --tag default-threads --strategies check-then-insert

Writes results/m1_race_runs[_<tag>].csv and results/m1_race_summary[_<tag>].json.
"""
import argparse
import asyncio
import csv
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

import httpx
import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

from app import CHARGE_DELAY, DSN  # noqa: E402

PORT = 58101
BASE_URL = f"http://127.0.0.1:{PORT}"
PATHS = {
    "check-then-insert": "/race/check-then-insert",
    "on-conflict": "/charges",
    "redis-set-nx": "/race/redis-set-nx",
}
BODY = {"amount": 1000, "currency": "usd"}


async def burst(lanes: list[httpx.AsyncClient], path: str, client_id: str) -> dict:
    headers = {"X-Client-Id": client_id, "Idempotency-Key": str(uuid.uuid4())}
    t0 = time.perf_counter()
    resps = await asyncio.gather(*(lane.post(path, json=BODY, headers=headers) for lane in lanes))
    wall_ms = (time.perf_counter() - t0) * 1000
    counts = {"new_201": 0, "replayed": 0, "conflict_409": 0, "error_500": 0, "other": 0}
    for resp in resps:
        if resp.status_code == 201 and resp.headers.get("Idempotent-Replayed") == "true":
            counts["replayed"] += 1
        elif resp.status_code == 201:
            counts["new_201"] += 1
        elif resp.status_code == 409:
            counts["conflict_409"] += 1
        elif resp.status_code == 500:
            counts["error_500"] += 1
        else:
            counts["other"] += 1
    return {**counts, "wall_ms": round(wall_ms, 1)}


async def main(args: argparse.Namespace) -> list[dict]:
    lanes = [httpx.AsyncClient(base_url=BASE_URL, limits=httpx.Limits(max_connections=1),
                               timeout=60) for _ in range(max(args.n))]
    rows = []
    try:
        # Open every lane's keep-alive connection, and make the server start its worker
        # threads, before the first measured burst.
        await asyncio.gather(*(lane.get("/health") for lane in lanes))
        await asyncio.gather(*(lane.get("/warm-threads") for lane in lanes))
        threads = (await lanes[0].get("/health")).json()["threads"]
        with psycopg.connect(DSN, autocommit=True) as pg:
            pg.execute((HERE / "schema.sql").read_text())
            for strategy in args.strategies:  # one unrecorded burst each
                await burst(lanes, PATHS[strategy], f"m1-warmup-{uuid.uuid4().hex[:8]}")
            for run in range(1, args.runs + 1):
                for n in args.n:
                    for strategy in args.strategies:
                        client_id = f"m1-{args.tag}-{strategy}-{n}-{run}-{uuid.uuid4().hex[:8]}"
                        result = await burst(lanes[:n], PATHS[strategy], client_id)
                        await asyncio.sleep(0.3)  # let stragglers finish before counting
                        charged = pg.execute("SELECT count(*) FROM charges WHERE client_id = %s",
                                             (client_id,)).fetchone()[0]
                        row = {"strategy": strategy, "concurrency": n, "run": run,
                               "charges": charged, **result, "server_threads": threads}
                        rows.append(row)
                        print(row, flush=True)
    finally:
        for lane in lanes:
            await lane.aclose()
    return rows


def summarise(rows: list[dict]) -> dict:
    out = {}
    for strategy in dict.fromkeys(r["strategy"] for r in rows):
        for n in sorted({r["concurrency"] for r in rows}):
            sel = [r for r in rows if r["strategy"] == strategy and r["concurrency"] == n]
            if not sel:
                continue
            stats = {"runs": len(sel)}
            for field in ("charges", "conflict_409", "replayed", "error_500", "new_201", "wall_ms"):
                vals = [r[field] for r in sel]
                stats[field] = {"median": statistics.median(vals), "min": min(vals), "max": max(vals)}
            stats["runs_with_duplicates"] = sum(1 for r in sel if r["charges"] > 1)
            stats["total_charges"] = sum(r["charges"] for r in sel)
            out[f"{strategy}@{n}"] = stats
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--n", type=int, nargs="+", default=[50, 100])
    ap.add_argument("--strategies", nargs="+", default=list(PATHS))
    ap.add_argument("--threads", type=int, default=200, help="AnyIO thread limit in the server")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    suffix = f"_{args.tag}" if args.tag else ""
    env = {**os.environ, "RACE_THREADS": str(args.threads)}
    # The server log fills up with check-then-insert's UniqueViolation tracebacks.
    log = open(pathlib.Path(tempfile.gettempdir()) / f"idem_m1_server{suffix}.log", "w")
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "race_app:app", "--port", str(PORT),
         "--log-level", "warning", "--no-access-log", "--timeout-keep-alive", "600"],
        cwd=HERE, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        for _ in range(100):
            try:
                httpx.get(f"{BASE_URL}/health", timeout=1)
                break
            except httpx.TransportError:
                time.sleep(0.2)
        with measuring("idem-m1-race"):
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            rows = asyncio.run(main(args))
    finally:
        server.terminate()
        server.wait(timeout=10)
        log.close()

    with open(HERE / "results" / f"m1_race_runs{suffix}.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "started": started,
        "setup": {"charge_delay_s": CHARGE_DELAY, "server_threads": args.threads,
                  "runs": args.runs, "concurrency": args.n, "python": platform.python_version(),
                  "client": "asyncio + one httpx.AsyncClient per concurrent request, warm keep-alive connections",
                  "server": "uvicorn race_app:app, 1 process, sync endpoints, pooled psycopg connections"},
        "results": summarise(rows),
    }
    (HERE / "results" / f"m1_race_summary{suffix}.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["results"], indent=2))
