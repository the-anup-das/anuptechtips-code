"""M3: how long does it take to get a dropped table back? Trash vs vault, two table sizes.

For each table: RUNS soft drops, each followed by a restore from the trash, then RUNS hard
drops, each followed by a restore from the vault. Restores are timed end to end, each
including its own new connection; the trash statement is also timed on its own. Timing,
so it runs under the shared measure lock.

    python measure_restore.py   -> results/m3_restore.csv, results/m3_restore_summary.json
"""
import csv
import json
import pathlib
import statistics
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import lab  # noqa: E402
import vault  # noqa: E402

RUNS = 20
TABLES = ("executives", "courses_answer")  # 1,206 rows and 1,943,200 rows


def ms_since(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 3)


def measure(table: str, vault_dir: pathlib.Path) -> tuple[list[dict], int]:
    lab.reset(prod=(table,))
    rows = lab.ROWS[table]
    samples = []

    def sample(method: str, run: int, ms: float) -> None:
        samples.append({"table": table, "rows": rows, "method": method, "run": run, "ms": ms})

    for run in range(1, RUNS + 1):
        with lab.connect(lab.OPERATOR) as operator:
            start = time.perf_counter()
            operator.execute("SELECT ops.soft_drop(%s)", (table,))
            sample("soft drop (statement)", run, ms_since(start))
        start = time.perf_counter()
        with lab.connect(lab.OPERATOR) as operator:
            statement = time.perf_counter()
            operator.execute("SELECT ops.restore(%s)", (table,))
            sample("trash restore (statement)", run, ms_since(statement))
        sample("trash restore", run, ms_since(start))

    for run in range(1, RUNS + 1):
        start = time.perf_counter()
        vault.backup(table, vault_dir)
        sample("vault backup", run, ms_since(start))
        with lab.connect(lab.OWNER) as owner:
            owner.execute(f"DROP TABLE prod.{table}")
        start = time.perf_counter()
        vault.restore(table, vault_dir)
        sample("vault restore", run, ms_since(start))

    with lab.connect(lab.OWNER) as owner:
        assert lab.original_rows(owner, table) == rows
    size = (vault_dir / f"{table}.copy").stat().st_size
    return samples, size


def main() -> None:
    lab.create()
    samples, sizes = [], {}
    with measuring("agentdel-m3-restore"), tempfile.TemporaryDirectory() as tmp:
        for table in TABLES:
            table_samples, sizes[table] = measure(table, pathlib.Path(tmp))
            samples += table_samples
            print(f"{table} done", flush=True)
    lab.reset()

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m3_restore.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(samples[0]))
        writer.writeheader()
        writer.writerows(samples)

    summary = []
    for table in TABLES:
        for method in dict.fromkeys(s["method"] for s in samples):
            values = [s["ms"] for s in samples if (s["table"], s["method"]) == (table, method)]
            summary.append({"table": table, "rows": lab.ROWS[table], "method": method,
                            "runs": len(values), "median_ms": round(statistics.median(values), 2),
                            "min_ms": min(values), "max_ms": max(values)})
            print(summary[-1])
    (out / "m3_restore_summary.json").write_text(json.dumps(
        {"runs": RUNS, "vault_file_bytes": sizes, "results": summary}, indent=2))


if __name__ == "__main__":
    main()
