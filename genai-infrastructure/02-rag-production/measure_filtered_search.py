"""M3: how many rows a permission-filtered vector search returns on pgvector, and what the
fix costs.

200,000 synthetic chunks (20,000 documents x 10 chunks, 384 dimensions) in the post's schema.
Every document is readable by one of 100 groups, so a user in 1, 10 or 50 groups may read
about 1%, 10% or 50% of the corpus. 200 queries per setting with the post's SQL, once with
LIMIT 10 and once with LIMIT 50 (what the query path asks each search for):

  off            pgvector's default: no iterative scan, hnsw.ef_search = 40
  relaxed_order  SET hnsw.iterative_scan = relaxed_order   (what retrieval.connect() sets)
  strict_order   SET hnsw.iterative_scan = strict_order
  exact          no HNSW: filter first, then sort every allowed row (also the ground truth)

Rows returned and recall come from one pass over the queries (they don't change between
passes). Latency is measured in 20 rounds; the summary holds the median, minimum and maximum
of the per-round p50 and p95.

    python measure_filtered_search.py            # build the table if needed, then measure
    python measure_filtered_search.py --rebuild  # drop and reload the table first
"""
import csv
import json
import pathlib
import re
import statistics
import sys
import time

import numpy as np
import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

import harness  # noqa: E402
import retrieval  # noqa: E402
import synth  # noqa: E402
from retrieval import DENSE  # noqa: E402

SCHEMA = "bench"
DOCS, CHUNKS_PER_DOC, GROUPS = 20_000, 10, 100
QUERIES, ROUNDS = 200, 20
LIMITS = (10, 50)
SELECTIVITY = {1: 1, 10: 10, 50: 50}          # label (%) -> number of groups the user is in
MODES = ("off", "relaxed_order", "strict_order", "exact")
MODEL = "synthetic-384"

# The same search without the vector index: "+ 0" stops the planner from using HNSW for the
# ORDER BY, so it filters first (GIN on acl, or a sequential scan) and sorts what is left.
EXACT = DENSE.replace("ORDER BY distance\n", "ORDER BY (embedding <=> %(query)s::vector) + 0\n")
assert EXACT != DENSE


def load(conn: psycopg.Connection) -> None:
    rng = np.random.default_rng(1)
    centres = synth.make_centres()
    topic_of_doc = rng.integers(0, len(centres), DOCS)       # a document is about one subtopic
    group_of_doc = rng.integers(0, GROUPS, DOCS)             # and one group may read it
    vectors = synth.noisy(np.repeat(centres[topic_of_doc], CHUNKS_PER_DOC, axis=0), rng)

    hnsw = next(line for line in (HERE / "schema.sql").read_text().splitlines()
                if "USING hnsw" in line)
    conn.execute("DROP INDEX chunks_embedding")   # load first, build the graph once afterwards
    with conn.cursor() as cur:
        with cur.copy("COPY documents (tenant_id, doc_id, version) FROM STDIN") as copy:
            for doc in range(DOCS):
                copy.write_row((harness.TENANT, f"doc-{doc:05d}", 1))
        with cur.copy("COPY chunks (tenant_id, doc_id, version, chunk_hash, position, title, "
                      "url, content, acl, embed_model, embedding) FROM STDIN") as copy:
            for i, vector in enumerate(vectors):
                doc, position = divmod(i, CHUNKS_PER_DOC)
                copy.write_row((harness.TENANT, f"doc-{doc:05d}", 1, f"{i:064x}", position,
                                f"Document {doc}", f"https://docs.example/{doc}",
                                f"synthetic chunk {i}", f"{{g{group_of_doc[doc]:02d}}}", MODEL,
                                synth.to_text(vector)))
    # Docker's default /dev/shm is 64 MB, too small for a parallel HNSW build.
    conn.execute("SET max_parallel_maintenance_workers = 0")
    conn.execute("SET maintenance_work_mem = '1GB'")
    started = time.perf_counter()
    conn.execute(hnsw)
    print(f"HNSW index built in {time.perf_counter() - started:.0f} s")
    conn.execute("VACUUM ANALYZE chunks")


def connect(rebuild: bool) -> psycopg.Connection:
    with psycopg.connect(harness.DSN, autocommit=True) as admin:
        exists = admin.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.chunks",)).fetchone()[0]
    if rebuild or not exists:
        harness.setup_schema(SCHEMA)
    conn = retrieval.connect(harness.schema_dsn(SCHEMA))   # the post's connection settings
    if conn.execute("SELECT count(*) FROM chunks").fetchone()[0] != DOCS * CHUNKS_PER_DOC:
        conn.execute("TRUNCATE chunks, documents")
        load(conn)
    return conn


def set_mode(conn: psycopg.Connection, mode: str) -> str:
    conn.execute("SET hnsw.ef_search = 40")
    conn.execute(f"SET hnsw.iterative_scan = {'off' if mode == 'exact' else mode}")
    return EXACT if mode == "exact" else DENSE


def plan_of(conn: psycopg.Connection, sql: str, args: dict) -> str:
    lines = [row[0] for row in conn.execute("EXPLAIN " + sql, args).fetchall()]
    nodes = [re.sub(r"\s+\(cost.*", "", line).replace("->", "").strip()
             for line in lines if "->" in line and "CTE Scan" not in line]
    return " > ".join(nodes)


def main() -> None:
    conn = connect(rebuild="--rebuild" in sys.argv)
    conn.execute("SELECT '[1]'::vector")   # load pgvector so its settings exist
    # Vectors travel as compact text: on Docker Desktop, a request over 8 kB waits 50 ms in
    # the port proxy, which would drown the numbers we are after.
    queries = [synth.to_text(vector) for vector in synth.clustered(QUERIES, seed=2)]
    total = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]

    def args(query: str, groups: int, k: int) -> dict:
        return {"query": query, "tenant": harness.TENANT, "model": MODEL, "k": k,
                "groups": [f"g{g:02d}" for g in range(groups)]}

    def run(mode: str, groups: int, k: int) -> tuple[list[list[str]], list[float]]:
        sql = set_mode(conn, mode)
        ids, millis = [], []
        for query in queries:
            started = time.perf_counter()
            rows = conn.execute(sql, args(query, groups, k)).fetchall()
            millis.append((time.perf_counter() - started) * 1000)
            ids.append([row[0] for row in rows])
        return ids, millis

    configs = [(k, label, groups, mode) for k in LIMITS
               for label, groups in SELECTIVITY.items() for mode in MODES]
    summary, per_query, truth = [], [], {}
    for k, label, groups, mode in configs:
        if (k, label) not in truth:
            truth[k, label], _ = run("exact", groups, k)
        allowed = conn.execute("SELECT count(*) FROM chunks WHERE acl && %s::text[]",
                               (args("", groups, k)["groups"],)).fetchone()[0]
        ids, _ = run(mode, groups, k)
        rows = [len(found) for found in ids]
        recall = [len(set(found) & set(true)) / k for found, true in zip(ids, truth[k, label])]
        per_query += [{"k": k, "selectivity_pct": label, "mode": mode, "query": q, "rows": n,
                       "recall": r} for q, (n, r) in enumerate(zip(rows, recall))]
        summary.append({
            "k": k, "selectivity_pct": label, "allowed_chunks": allowed,
            "allowed_share": round(allowed / total, 4), "mode": mode,
            "plan": plan_of(conn, set_mode(conn, mode), args(queries[0], groups, k)),
            "rows_mean": round(statistics.mean(rows), 2), "rows_min": min(rows),
            "rows_max": max(rows), "queries_with_fewer_than_k": sum(n < k for n in rows),
            "queries_with_zero_rows": sum(n == 0 for n in rows),
            "recall_at_k": round(statistics.mean(recall), 3)})
        print(summary[-1])

    rounds = []
    with measuring("rag-filtered-search"):
        for k, _, groups, mode in configs:   # warm-up pass, not recorded
            run(mode, groups, k)
        for round_no in range(1, ROUNDS + 1):
            for k, label, groups, mode in configs:
                _, millis = run(mode, groups, k)
                rounds.append({"round": round_no, "k": k, "selectivity_pct": label,
                               "mode": mode,
                               "p50_ms": round(float(np.percentile(millis, 50)), 3),
                               "p95_ms": round(float(np.percentile(millis, 95)), 3)})
            print(f"round {round_no}/{ROUNDS} done")

    for entry in summary:
        mine = [r for r in rounds if (r["k"], r["selectivity_pct"], r["mode"]) ==
                (entry["k"], entry["selectivity_pct"], entry["mode"])]
        for stat in ("p50_ms", "p95_ms"):
            values = [r[stat] for r in mine]
            entry[stat] = {"median": round(statistics.median(values), 2),
                           "min": round(min(values), 2), "max": round(max(values), 2)}

    results = HERE / "results"
    results.mkdir(exist_ok=True)
    for name, rows in (("m3_filtered_search_per_query.csv", per_query),
                       ("m3_filtered_search_latency_rounds.csv", rounds)):
        with open(results / name, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    version = conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
    (results / "m3_filtered_search.json").write_text(json.dumps({
        "chunks": total, "documents": DOCS, "dimensions": synth.DIM, "groups": GROUPS,
        "queries": QUERIES, "limits": LIMITS, "rounds": ROUNDS, "ef_search": 40,
        "postgres": conn.execute("SHOW server_version").fetchone()[0],
        "pgvector": version.fetchone()[0], "results": summary}, indent=2) + "\n")
    print("wrote results/m3_filtered_search.json")


if __name__ == "__main__":
    main()
