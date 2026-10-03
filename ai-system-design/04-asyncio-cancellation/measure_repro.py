"""M2 and M4: run repro_cancel_then_get.py 20 times per redis-py and count how often the
second GET came back with the first GET's value. Each redis-py is run twice: with the
second GET right after the cancel, and with a 300 ms pause before it.

    python measure_repro.py                        # the installed redis-py
    python measure_repro.py --old-python <path>    # also redis-py 4.5.1 (see the README)

Writes results/repro_runs.csv, results/repro_summary.json and, per redis-py version, the
script's output from its first run without a pause to
results/repro_output_redis-py-<version>.txt.
"""
import argparse
import csv
import json
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
COLUMNS = ["redis_py", "pause_ms", "run", "bar", "ping", "foo", "connections_opened"]
PAUSES_MS = [0, 300]


def one_run(python: str, pause_ms: int) -> tuple[dict, str]:
    """One run: the parsed JSON line, and what the script printed before it."""
    done = subprocess.run([python, "repro_cancel_then_get.py", "--json", "--pause-ms",
                           str(pause_ms)], cwd=HERE, capture_output=True, text=True)
    if done.returncode != 0 or "sent, reply not read" not in done.stdout:
        raise RuntimeError(done.stdout + done.stderr[-2000:])
    *printed, last = done.stdout.strip().splitlines()
    return json.loads(last), "\n".join(printed) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--old-python", default=os.environ.get("REDIS_451_PYTHON"),
                        help="a Python that has redis==4.5.1 installed (optional)")
    args = parser.parse_args()

    results = HERE / "results"
    results.mkdir(exist_ok=True)
    rows = []
    for python in filter(None, [args.old_python, sys.executable]):
        for pause_ms in PAUSES_MS:
            for run in range(1, args.runs + 1):
                result, printed = one_run(python, pause_ms)
                rows.append({"run": run, **result, "pause_ms": pause_ms})
                print(rows[-1], flush=True)
                if run == 1 and pause_ms == 0:  # keep one transcript per version
                    name = f"repro_output_redis-py-{result['redis_py']}.txt"
                    shown = "$REDIS_451_PYTHON" if python == args.old_python else "python"
                    (results / name).write_text(f"$ {shown} repro_cancel_then_get.py\n" + printed,
                                                newline="\n")

    with open(results / "repro_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    summary = {}
    for version, pause_ms in dict.fromkeys((row["redis_py"], row["pause_ms"]) for row in rows):
        mine = [row for row in rows if (row["redis_py"], row["pause_ms"]) == (version, pause_ms)]
        pause = f", {pause_ms} ms pause before the second GET" if pause_ms else ""
        summary[f"redis-py {version}{pause}"] = {
            "runs": len(mine),
            "second_get_returned_first_gets_value": sum(r["bar"] == "b'foo'" for r in mine),
            "second_get_returned_its_own_value": sum(r["bar"] == "b'bar'" for r in mine),
            "outputs_seen": sorted({f"bar: {r['bar']} | ping: {r['ping']} | foo: {r['foo']}"
                                    for r in mine}),
            "connections_opened": sorted({r["connections_opened"] for r in mine}),
        }
    (results / "repro_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
