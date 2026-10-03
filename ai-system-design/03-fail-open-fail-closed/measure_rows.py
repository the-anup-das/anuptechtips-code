"""M1: how many rows the generator's query returns before and after the permission change,
which grants matter, and what the schema filter does. Counts only, so no measure lock.

Writes results/m1_rows.csv and results/m1_rows.json.
"""
import csv
import json
import pathlib

import psycopg

import lab
from generator import FILTERED, UNFILTERED, feature_names

HERE = pathlib.Path(__file__).resolve().parent
RUNS = 5
ROLE = "feature_gen"
SCENARIOS = [
    ("before the change", []),
    ("USAGE on schema r0 only", ["GRANT USAGE ON SCHEMA r0 TO feature_gen"]),
    ("SELECT on r0.http_requests_features", [
        "GRANT SELECT ON r0.http_requests_features TO feature_gen"]),
    ("SELECT on the table and USAGE on the schema", [
        "GRANT SELECT ON r0.http_requests_features TO feature_gen",
        "GRANT USAGE ON SCHEMA r0 TO feature_gen"]),
    ("INSERT on the table", ["GRANT INSERT ON r0.http_requests_features TO feature_gen"]),
    ("UPDATE on the table", ["GRANT UPDATE ON r0.http_requests_features TO feature_gen"]),
    ("REFERENCES on the table", [
        "GRANT REFERENCES ON r0.http_requests_features TO feature_gen"]),
    ("DELETE on the table", ["GRANT DELETE ON r0.http_requests_features TO feature_gen"]),
    ("TRUNCATE on the table", ["GRANT TRUNCATE ON r0.http_requests_features TO feature_gen"]),
    ("TRIGGER on the table", ["GRANT TRIGGER ON r0.http_requests_features TO feature_gen"]),
]


def can_read_r0(conn: psycopg.Connection) -> bool:
    try:
        with conn.transaction():
            conn.execute("SET LOCAL ROLE feature_gen")
            conn.execute("SELECT count(*) FROM r0.http_requests_features")
        return True
    except psycopg.errors.InsufficientPrivilege:
        return False


def main() -> None:
    rows = []
    with lab.connect() as conn:
        for run in range(1, RUNS + 1):
            for scenario, grants in SCENARIOS:
                lab.reset_schema(conn)
                for grant in grants:
                    conn.execute(grant)
                unfiltered = feature_names(conn, ROLE, UNFILTERED)
                rows.append({
                    "run": run, "scenario": scenario,
                    "rows_unfiltered": len(unfiltered),
                    "distinct_names": len(set(unfiltered)),
                    "rows_with_schema_filter": len(feature_names(conn, ROLE, FILTERED)),
                    "role_can_read_r0_table": can_read_r0(conn),
                })
        lab.reset_schema(conn)

    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m1_rows.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    summary = {}
    for scenario, _ in SCENARIOS:
        mine = [r for r in rows if r["scenario"] == scenario]
        assert all(r == {**mine[0], "run": r["run"]} for r in mine), "runs disagree"
        summary[scenario] = {k: v for k, v in mine[0].items() if k not in ("run", "scenario")}
    (out / "m1_rows.json").write_text(json.dumps({"runs": RUNS, "scenarios": summary}, indent=2))

    print(f"{'scenario':46} unfiltered  distinct  filtered  can read r0")
    for scenario, s in summary.items():
        print(f"{scenario:46} {s['rows_unfiltered']:>10} {s['distinct_names']:>9} "
              f"{s['rows_with_schema_filter']:>9}  {s['role_can_read_r0_table']}")


if __name__ == "__main__":
    main()
