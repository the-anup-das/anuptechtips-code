"""M1, M2 and M4 (and the baseline for M3): the load test, once per client and seed.

Each run is `load_test.py` in a fresh process: 10,000 reads from 200 tasks through a
10-connection pool behind the delay proxy, with random 5-120 ms timeouts. Writes
results/load_runs.csv and results/load_summary.json.

    python measure_load.py                        # toy pools and the installed redis-py
    python measure_load.py --old-python <path>    # also redis-py 4.5.1 (see the README)
    python measure_load.py --resume               # keep finished runs, do only the missing ones
"""
import argparse
import csv
import json
import os
import pathlib
import statistics
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

SETUP = {"requests": 10_000, "tasks": 200, "pool": 10, "delay_ms": 5, "timeout_ms": [5, 120],
         "think_ms": 600}
WORKLOAD = ["--requests", "10000", "--tasks", "200", "--pool", "10", "--delay-ms", "5",
            "--timeout-ms", "5", "120", "--think-ms", "600"]
# name, load_test.py arguments, needs the old redis-py
VARIANTS = [
    ("toy-buggy", ["--client", "toy-buggy"], False),
    ("toy-fixed", ["--client", "toy-fixed"], False),
    ("toy-buggy+owner-check", ["--client", "toy-buggy", "--owner-check"], False),
    ("redis-py-4.5.1", ["--client", "redis"], True),
    ("redis-py-4.5.1+owner-check", ["--client", "redis", "--owner-check"], True),
    ("redis-py", ["--client", "redis"], False),
    ("redis-py+owner-check", ["--client", "redis", "--owner-check"], False),
    ("redis-py-lean-handshake", ["--client", "redis", "--lean-handshake"], False),
    ("redis-py-no-timeouts", ["--client", "redis", "--no-timeouts"], False),
    ("toy-fixed-no-timeouts", ["--client", "toy-fixed", "--no-timeouts"], False),
]
COUNTS = ["correct", "wrong_owner", "misses", "owner_mismatches_caught", "timed_out", "errors",
          "connections_opened", "p50_ms", "p99_ms", "seconds"]
COLUMNS = ["variant", "run", "seed", "library", "python", "requests", *COUNTS, "error_types",
           "undecodable_replies"]


def one_run(python: str, load_args: list[str], seed: int) -> dict:
    done = subprocess.run([python, "load_test.py", *load_args, *WORKLOAD, "--seed", str(seed)],
                          cwd=HERE, capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(done.stderr[-2000:])
    result = json.loads(done.stdout.strip().splitlines()[-1])
    for name in ("error_types", "undecodable_replies"):
        result[name] = json.dumps(result[name], sort_keys=True)
    return result


def read_rows(path: pathlib.Path) -> list[dict]:
    rows = list(csv.DictReader(open(path)))
    for row in rows:
        for name in ("run", "seed", "requests", *COUNTS):
            row[name] = float(row[name]) if "." in row[name] else int(row[name])
    return rows


def summarize(rows: list[dict]) -> dict:
    out = {"runs": len(rows), "library": rows[0]["library"]}
    for name in COUNTS:
        values = [row[name] for row in rows]
        out[name] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    for name in ("error_types", "undecodable_replies"):
        total: dict[str, int] = {}
        for row in rows:
            for kind, count in json.loads(row[name]).items():
                total[kind] = total.get(kind, 0) + count
        out[f"{name}_all_runs"] = total
    out["runs_with_wrong_owner_reads"] = sum(1 for row in rows if row["wrong_owner"] > 0)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--old-python", default=os.environ.get("REDIS_451_PYTHON"),
                        help="a Python that has redis==4.5.1 installed (optional)")
    parser.add_argument("--resume", action="store_true",
                        help="keep the runs already in results/load_runs.csv")
    args = parser.parse_args()

    variants = [v for v in VARIANTS if args.old_python or not v[2]]
    results = HERE / "results"
    results.mkdir(exist_ok=True)
    path = results / "load_runs.csv"
    rows = read_rows(path) if args.resume and path.exists() else []
    finished = {(row["variant"], row["run"]) for row in rows}
    # Round-robin over the variants, in reverse order on even rounds: a run that opens many
    # connections slows the run after it a little, and this way nobody is always "after".
    todo = [(run, variant) for run in range(1, args.runs + 1)
            for variant in (variants if run % 2 else variants[::-1])
            if (variant[0], run) not in finished]

    with open(path, "a" if rows else "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        if not rows:
            writer.writeheader()
        with measuring("asyncio-cancellation-load", poll=0.25):  # one benchmark at a time here
            for run, (name, load_args, old) in todo:
                result = one_run(args.old_python if old else sys.executable, load_args, seed=run)
                rows.append({"variant": name, "run": run, **result})
                writer.writerow(rows[-1])
                f.flush()
                print(f"run {run:2d} {name:28s} wrong_owner={result['wrong_owner']:5d} "
                      f"timed_out={result['timed_out']:5d} errors={result['errors']:3d} "
                      f"connections={result['connections_opened']:4d} p99={result['p99_ms']} ms",
                      flush=True)

    summary = {name: summarize([r for r in rows if r["variant"] == name])
               for name in dict.fromkeys(r["variant"] for r in rows)}
    (results / "load_summary.json").write_text(
        json.dumps({"setup": SETUP, "variants": summary}, indent=2) + "\n")
    print("wrote results/load_runs.csv and results/load_summary.json")


if __name__ == "__main__":
    main()
