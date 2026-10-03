"""Two latency surprises of this setup, measured so nobody has to take them on trust.

On Docker Desktop for Windows, a request of more than about 8 kB to a published Postgres port
waits a flat ~50 ms in the port proxy, however fast the query is. So does every psycopg
`executemany`, which sends its statements as a pipeline. A 384-dimension vector sent as Python
floats crosses the 8 kB line; the same vector at float4 precision does not.

20 rounds of 20 requests per case; the summary holds the median, minimum and maximum of the
per-round medians.
"""
import json
import pathlib
import statistics
import sys
import time

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import synth  # noqa: E402
from indexer import to_vector  # noqa: E402
from retrieval import DSN  # noqa: E402

SIZES = (1000, 4000, 6000, 8000, 9000, 12000, 20000)
ROUNDS, REQUESTS = 20, 20


def timed(fn) -> float:
    started = time.perf_counter()
    fn()
    return (time.perf_counter() - started) * 1000


def summary(values: list[float]) -> dict[str, float]:
    return {"median": round(statistics.median(values), 2), "min": round(min(values), 2),
            "max": round(max(values), 2)}


def main() -> None:
    vector = [float(v) for v in synth.clustered(1, seed=3)[0]]
    as_python_floats = "[" + ",".join(map(repr, vector)) + "]"
    vector_bytes = {"python floats, full precision": len(as_python_floats),
                    "to_vector(), float4 precision": len(to_vector(vector))}

    by_size = {size: [] for size in SIZES}
    inserts = {"executemany, 3 rows": [], "3 x execute": []}
    with psycopg.connect(DSN, autocommit=True) as conn, measuring("rag-request-size"):
        conn.execute("CREATE TEMP TABLE request_size_probe (n int)")
        rows = [(1,), (2,), (3,)]

        def many() -> None:
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO request_size_probe VALUES (%s)", rows)

        def one_by_one() -> None:
            for row in rows:
                conn.execute("INSERT INTO request_size_probe VALUES (%s)", row)

        for _ in range(ROUNDS):
            for size in SIZES:
                payload = "x" * size
                by_size[size].append(statistics.median(
                    timed(lambda: conn.execute("SELECT length(%s::text)", (payload,)).fetchall())
                    for _ in range(REQUESTS)))
            inserts["executemany, 3 rows"].append(
                statistics.median(timed(many) for _ in range(REQUESTS)))
            inserts["3 x execute"].append(
                statistics.median(timed(one_by_one) for _ in range(REQUESTS)))

    result = {
        "rounds": ROUNDS, "requests_per_round": REQUESTS,
        "median_ms_by_request_bytes": {str(size): summary(v) for size, v in by_size.items()},
        "median_ms_three_inserts": {name: summary(v) for name, v in inserts.items()},
        "bytes_of_a_384_dimension_query_vector": vector_bytes}
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "request_size.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=1))


if __name__ == "__main__":
    main()
