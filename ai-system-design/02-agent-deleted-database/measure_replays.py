"""M1: three incident replays against five setups, at full size, RUNS times each.

Per run: the 15 replay x setup combinations, the DataTalks.Club replay again with the
48-hour window expired (setups 4 and 5), and the PocketOS replay again with one leftover
all-powerful token on disk (setups 3 to 5). The outcomes are counts read back from
Postgres, so the script doesn't take the shared measure lock; restore times are measured
properly in measure_restore.py.

    python measure_replays.py   -> results/m1_replays.csv, results/m1_replays_summary.json
"""
import csv
import dataclasses
import json
import pathlib
import statistics
import tempfile

import redis

import lab
import replay

HERE = pathlib.Path(__file__).resolve().parent

RUNS = 5


def one_run(r: redis.Redis, workdir: pathlib.Path) -> list[tuple[str, replay.Outcome]]:
    outcomes = []
    for name in replay.REPLAYS:
        for setup in replay.SETUPS:
            outcomes.append(("as reported", replay.run(name, setup, r, workdir)))
    for setup in replay.SETUPS[3:]:
        outcomes.append(("noticed after the 48 hours",
                         replay.run("datatalks", setup, r, workdir, purged=True)))
    for setup in replay.SETUPS[2:]:
        outcomes.append(("one leftover owner token",
                         replay.run("pocketos", setup, r, workdir, leftover_token=lab.OWNER)))
    return outcomes


def main() -> None:
    lab.create()
    r = redis.Redis.from_url(lab.REDIS_URL)
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        for run in range(1, RUNS + 1):
            for variant, outcome in one_run(r, pathlib.Path(tmp)):
                rows.append({"run": run, "variant": variant, **dataclasses.asdict(outcome)})
            print(f"run {run}/{RUNS} done", flush=True)
    r.flushdb()
    lab.reset()

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m1_replays.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = []
    keys = dict.fromkeys((row["variant"], row["replay"], row["setup"]) for row in rows)
    for variant, name, setup in keys:
        group = [row for row in rows
                 if (row["variant"], row["replay"], row["setup"]) == (variant, name, setup)]
        restores = [row["restore_ms"] for row in group if row["restore_ms"] is not None]
        first = group[0]
        summary.append({
            "variant": variant, "replay": name, "setup": setup, "setup_name": first["setup_name"],
            "runs": len(group),
            "identical_in_every_run": len({(row["effect"], row["stopped_by"], row["error"],
                                            row["rows_recovered"], row["recovered_from"])
                                           for row in group}) == 1,
            "effect": first["effect"], "stopped_by": first["stopped_by"], "error": first["error"],
            "calls": first["calls"], "calls_refused": first["calls_refused"],
            "rows_before": first["rows_before"], "rows_after": first["rows_after"],
            "rows_recovered": first["rows_recovered"], "pct_lost": first["pct_lost"],
            "recovered_from": first["recovered_from"],
            "restore_ms_median": round(statistics.median(restores), 1) if restores else None,
            "restore_ms_min": min(restores, default=None),
            "restore_ms_max": max(restores, default=None),
        })
    (out / "m1_replays_summary.json").write_text(json.dumps(summary, indent=2))

    for row in summary:
        restore = f"{row['recovered_from']} {row['restore_ms_median']} ms" if row[
            "recovered_from"] else "-"
        print(f"{row['variant']:27} {row['replay']:10} setup {row['setup']}: "
              f"{row['effect']:11} lost {row['pct_lost']:5}%  "
              f"stopped by {row['stopped_by'] or '-':5}  restore: {restore}  "
              f"identical: {row['identical_in_every_run']}")


if __name__ == "__main__":
    main()
