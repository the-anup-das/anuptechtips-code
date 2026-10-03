# 02 · RAG architecture in production (PostgreSQL + pgvector)

Code, tests and measurements for the post
[RAG Architecture in Production: A Practical Guide for Engineers](https://anuptechtips.com/rag-architecture-production/).

It shows the parts of a retrieval-augmented generation system that a demo skips:

- **An index that can forget** (`schema.sql`, `indexer.py`): one Postgres row per chunk holds the
  text, the vector, the keyword entry and the ACL. The sync worker applies change events that
  arrive late, twice or out of order: a version claim, a versioned replace in one transaction,
  tombstones for deletes, vectors reused for unchanged text, and a reconciliation job for events
  that never arrive.
- **Permission-aware hybrid search** (`retrieval.py`): keyword and vector search, both filtered by
  tenant and by the user's groups inside the SQL, with pgvector's iterative index scans switched
  on so that a filtered search returns the rows it was asked for.
- **The query path** (`query.py`): rewrite, both searches at once, reciprocal rank fusion, a
  reranker with a time limit and a fallback, numbered sources, `NO_ANSWER`, a citation check,
  per-stage timers and a cache key that includes the permission scope.
- **A retrieval gate for CI** (`tests/test_retrieval_gate.py`, `golden/`): recall on a golden set
  must not drop, and no restricted chunk may come back.
- **Two measurements**: what a filtered vector search returns at three scopes, and what three
  ingestion consumers still serve after documents are deleted.

No embedding model, reranker or LLM is needed. `embedder.py` is a deterministic hashing
stand-in, the reranker and the LLM are fakes in the tests, and the measurements use seeded
synthetic vectors (`synth.py`).

## Files

| File | What it is |
|---|---|
| `schema.sql` | `documents` (version or tombstone per document) and `chunks` (text, `tsvector`, ACL, model name, `vector(384)`) with HNSW and GIN indexes |
| `chunking.py` | structure-aware chunker: Markdown headings, whole paragraphs, title and section path in front |
| `embedder.py` | `HashingEmbedder`, the stand-in for an embedding model |
| `indexer.py` | `Event`, `apply` (the ingestion consumer) and `reconcile` |
| `naive_indexer.py` | `append_only` and `last_event_wins`, the two consumers that go wrong (for the drill only) |
| `retrieval.py` | `connect`, the `DENSE`, `EXACT` and `KEYWORD` SQL, `dense_search`, `keyword_search`, `hybrid_search` |
| `query.py` | `answer`: the whole query path, with plain callables for search, rerank and the LLM |
| `golden/` | an 11-page handbook, its ACLs, 23 golden questions and the recall baseline |
| `harness.py` | throwaway schemas and the handbook as events, for the tests and the measurements |
| `synth.py` | seeded, clustered synthetic vectors |
| `measure_filtered_search.py` | M3: rows returned, recall and latency of a filtered search, by scope and setting |
| `measure_delete_drill.py` | M5: what three consumers still serve after edits, revokes and deletes |
| `measure_request_size.py` | the Docker Desktop request-size quirk described below |
| `tests/` | pytest against the pgvector container, one test per behavior the post describes |
| `results/` | raw CSV/JSON from every run |

## Run it

Start the database from the series folder, one level up: `docker compose up -d` (see
`../docker-compose.yml`: PostgreSQL 18 with pgvector 0.8.6 on port 55433, database `rag`). You
need Python 3.12 with `psycopg[binary]`, `numpy` and `pytest`. Then, from the repo root:

```shell
cd genai-infrastructure/02-rag-production
pytest -q
python measure_filtered_search.py
python measure_delete_drill.py
python measure_request_size.py
```

Every test creates its own schema and drops it. The measurements use the schemas `bench` and
`drill`. Override the connection with `RAG_DSN`.

## Results on this machine

### Filtered vector search (`measure_filtered_search.py`)

200,000 synthetic chunks (20,000 documents x 10 chunks, 384
dimensions), 100 groups, 200 queries per setting, HNSW with
`hnsw.ef_search = 40`. Rows and recall come from one pass; latency is the median
of 20 rounds' p50 and p95. Recall is measured against the exact search with the
same filter.

`LIMIT 10`:

| User may read | Setting | Rows returned (of 10) | Searches short of 10 | Empty | Recall@10 vs exact | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|
| 1% | default (off) | 0.47 | 200 | 148 | 0.045 | 1.01 | 1.26 |
| 1% | `relaxed_order` | 10 | 0 | 0 | 0.948 | 5.35 | 14.2 |
| 1% | `strict_order` | 10 | 0 | 0 | 0.833 | 5.63 | 20.67 |
| 1% | exact (no HNSW) | 10 | 0 | 0 | 1.000 | 1.74 | 2.1 |
| 10% | default (off) | 4.14 | 195 | 7 | 0.401 | 0.85 | 1.13 |
| 10% | `relaxed_order` | 10 | 0 | 0 | 0.954 | 1.02 | 1.6 |
| 10% | `strict_order` | 10 | 0 | 0 | 0.943 | 0.98 | 1.51 |
| 10% | exact (no HNSW) | 10 | 0 | 0 | 1.000 | 22.04 | 23.27 |
| 50% | default (off) | 9.99 | 1 | 0 | 0.942 | 0.87 | 1.08 |
| 50% | `relaxed_order` | 10 | 0 | 0 | 0.942 | 0.88 | 1.06 |
| 50% | `strict_order` | 10 | 0 | 0 | 0.942 | 0.88 | 1.04 |
| 50% | exact (no HNSW) | 10 | 0 | 0 | 1.000 | 90.95 | 113.93 |

`LIMIT 50` (what `answer()` asks each search for):

| User may read | Setting | Rows returned (of 50) | Searches short of 50 | Empty | Recall@50 vs exact | p50 ms | p95 ms |
|---|---|---|---|---|---|---|---|
| 1% | default (off) | 50 | 0 | 0 | 1.000 | 1.82 | 2.43 |
| 1% | `relaxed_order` | 50 | 0 | 0 | 1.000 | 1.72 | 2.26 |
| 1% | `strict_order` | 50 | 0 | 0 | 1.000 | 1.8 | 2.3 |
| 1% | exact (no HNSW) | 50 | 0 | 0 | 1.000 | 1.92 | 2.51 |
| 10% | default (off) | 4.16 | 200 | 7 | 0.081 | 1.04 | 1.27 |
| 10% | `relaxed_order` | 50 | 0 | 0 | 0.949 | 2.81 | 4.96 |
| 10% | `strict_order` | 50 | 0 | 0 | 0.896 | 2.71 | 4.95 |
| 10% | exact (no HNSW) | 50 | 0 | 0 | 1.000 | 22.07 | 23.46 |
| 50% | default (off) | 20.25 | 200 | 0 | 0.392 | 0.91 | 1.12 |
| 50% | `relaxed_order` | 50 | 0 | 0 | 0.952 | 1.13 | 1.4 |
| 50% | `strict_order` | 50 | 0 | 0 | 0.944 | 1.11 | 1.41 |
| 50% | exact (no HNSW) | 50 | 0 | 0 | 1.000 | 89.68 | 112.45 |

Without iterative scans an HNSW search returns at most `hnsw.ef_search` rows before the filter
is applied, so a filtered search comes back short. At `LIMIT 50` and a 1% scope the planner
chose the exact plan (filter through the GIN index on `acl`, then sort) for every setting,
which is why nothing is missing in those rows.

### Delete drill (`measure_delete_drill.py`)

2,000 documents; 400 edited, 100 lose a group,
100 deleted: 2,600 events, delivered late, out of order and one
in ten twice. Median of 20 runs (minimum to maximum where the runs differ).

| Consumer | Deleted docs still served (of 100) | Edited docs serving old text (of 400) | Revoked docs still readable (of 100) | Leftover chunks |
|---|---|---|---|---|
| `append_only` | 100 | 400 | 100 | 3000 |
| `last_event_wins` | 16 (10 to 22) | 58 (40 to 70) | 12.5 (7 to 19) | 435 (365 to 495) |
| `indexer.apply` (versioned) | 0 | 0 | 0 | 0 |
| versioned, 2% of the events lost | 2 (0 to 4) | 7 (4 to 11) | 2 (1 to 7) | 60 (30 to 85) |
| the same, after one `reconcile` | 0 | 0 | 0 | 0 |

### Request size on Docker Desktop (`measure_request_size.py`)

Median milliseconds for one `SELECT length(%s::text)` by size of the text parameter, 20
rounds of 20 requests:

| 1,000 bytes | 4,000 bytes | 6,000 bytes | 8,000 bytes | 9,000 bytes | 12,000 bytes | 20,000 bytes |
|---|---|---|---|---|---|---|
| 0.32 | 0.33 | 0.33 | 0.34 | 50 | 50 | 50.01 |

Three single-row inserts: 50 ms with `executemany`,
0.97 ms with three `execute` calls. A 384-dimension vector is
8,022 bytes as Python floats and
4,780 bytes through `to_vector()`.

## About the numbers

- **The vectors are synthetic.** `synth.py` draws unit vectors around 500 cluster centres
  (50 topics of 10 subtopics) with a fixed seed. That is enough for these two tests: the
  filtered-search shortfall comes from the order of operations (index scan first, filter
  second), and the delete drill asks which rows exist, not how good the embeddings are. Recall
  and latency on real embeddings will differ.
- **Docker's default `/dev/shm` is 64 MB**, too small for a parallel HNSW build
  (`could not resize shared memory segment`). The benchmark builds its index with
  `max_parallel_maintenance_workers = 0`; adding `shm_size: 1g` to the service works too.
- **Docker Desktop on Windows adds about 50 ms to any request over 8 kB** sent to a published
  Postgres port, and to every psycopg `executemany` (pipeline mode), however fast the statement
  is. That is why vectors are sent as text at float4 precision (`to_vector`) and why `apply`
  inserts chunk by chunk. On Linux, or with the database on a real network, use `executemany`
  or `COPY` for bulk loads.
- Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5
  (WSL2); Python 3.12, psycopg 3.3.6, PostgreSQL 18.6 and pgvector 0.8.6 in Docker. Timing runs
  take the shared lock in `../common/measure_lock.py`, so two benchmarks never overlap.

Found a case that breaks one of these? Open an issue or a pull request.
