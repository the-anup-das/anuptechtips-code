"""Hybrid retrieval in SQL on PostgreSQL + pgvector. Both searches filter by tenant and by
the user's groups inside the query, so nothing the user can't read ever leaves the database."""
import os

import psycopg

from indexer import Embedder, to_vector
from query import Chunk, Scope, rrf

DSN = os.environ.get("RAG_DSN", "postgresql://patterns:patterns@localhost:55433/rag")


def connect(dsn: str = DSN) -> psycopg.Connection:
    conn = psycopg.connect(dsn, autocommit=True)
    # Keep scanning the HNSW graph until LIMIT rows pass the filters, up to hnsw.max_scan_tuples.
    conn.execute("SET hnsw.iterative_scan = relaxed_order")
    # Plan every search with its real parameters: a cached generic plan can't see how
    # selective this user's groups are.
    conn.execute("SET plan_cache_mode = force_custom_plan")
    return conn


# relaxed_order may return rows slightly out of order, so sort again outside the CTE.
DENSE = """
WITH nearest AS MATERIALIZED (
    SELECT doc_id || '#' || position AS id, title, url, content,
           embedding <=> %(query)s::vector AS distance
    FROM chunks
    WHERE tenant_id = %(tenant)s
      AND acl && %(groups)s::text[]   -- the permission filter, inside the search
      AND embed_model = %(model)s     -- never compare vectors from two models
    ORDER BY distance
    LIMIT %(k)s
)
SELECT id, title, url, content FROM nearest ORDER BY distance + 0
"""

# For a user who can read only a small slice. "+ 0" stops the planner from using HNSW for the
# ORDER BY, so Postgres filters first and sorts every allowed row: exact, and never short.
EXACT = """
WITH nearest AS MATERIALIZED (
    SELECT doc_id || '#' || position AS id, title, url, content,
           embedding <=> %(query)s::vector AS distance
    FROM chunks
    WHERE tenant_id = %(tenant)s
      AND acl && %(groups)s::text[]   -- the permission filter, inside the search
      AND embed_model = %(model)s     -- never compare vectors from two models
    ORDER BY (embedding <=> %(query)s::vector) + 0
    LIMIT %(k)s
)
SELECT id, title, url, content FROM nearest ORDER BY distance + 0
"""

# Any word may match (OR), ranked by cover density. Exact, so no shortfall on this side.
KEYWORD = """
SELECT doc_id || '#' || position AS id, title, url, content
FROM chunks,
     CAST(replace(plainto_tsquery('english', %(question)s)::text, '&', '|') AS tsquery) AS q
WHERE tenant_id = %(tenant)s
  AND acl && %(groups)s::text[]
  AND tsv @@ q
ORDER BY ts_rank_cd(tsv, q) DESC
LIMIT %(k)s
"""


def dense_search(conn: psycopg.Connection, embed: Embedder,
                 question: str, scope: Scope, k: int, exact: bool = False) -> list[Chunk]:
    args = {"query": to_vector(embed([question])[0]), "tenant": scope.tenant_id,
            "groups": list(scope.groups), "model": embed.name, "k": k}
    rows = conn.execute(EXACT if exact else DENSE, args)
    return [Chunk(*row) for row in rows]


def keyword_search(conn: psycopg.Connection, question: str, scope: Scope, k: int) -> list[Chunk]:
    rows = conn.execute(KEYWORD, {"question": question, "tenant": scope.tenant_id,
                                  "groups": list(scope.groups), "k": k})
    return [Chunk(*row) for row in rows]


def hybrid_search(conn: psycopg.Connection, embed: Embedder,
                  question: str, scope: Scope, k: int) -> list[Chunk]:
    """Both searches on one connection, fused with RRF. The query path runs them at once."""
    return rrf([keyword_search(conn, question, scope, 50),
                dense_search(conn, embed, question, scope, 50)])[:k]
