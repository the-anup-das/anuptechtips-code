"""The ingestion consumer: apply the source system's change events to the index.
Events arrive at least once and in any order; applying them stays correct either way."""
import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

import psycopg

from chunking import chunk


@dataclass(frozen=True)
class Event:
    type: str                  # "upsert" (new text or new permissions) or "delete"
    tenant_id: str
    doc_id: str
    version: int               # set by the source; goes up on every change to the document
    title: str = ""
    url: str = ""
    text: str = ""
    acl: tuple[str, ...] = ()  # groups allowed to read the document


class Embedder(Protocol):
    name: str                  # model and version, stored with every chunk

    def __call__(self, texts: Sequence[str]) -> list[list[float]]: ...


# Take this version, unless the index already holds it or a newer one. No row back = stale.
CLAIM = """
INSERT INTO documents (tenant_id, doc_id, version, deleted)
VALUES (%(tenant)s, %(doc)s, %(version)s, %(deleted)s)
ON CONFLICT (tenant_id, doc_id) DO UPDATE
    SET version = excluded.version, deleted = excluded.deleted, updated_at = now()
    WHERE documents.version < excluded.version
RETURNING version
"""

INSERT_CHUNK = """
INSERT INTO chunks (tenant_id, doc_id, version, chunk_hash, position, title, url,
                    content, acl, embed_model, embedding)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)
ON CONFLICT DO NOTHING
"""


def to_vector(values: Sequence[float]) -> str:
    """pgvector's text input, at float4 precision (the type stores 4-byte floats)."""
    return "[" + ",".join(f"{value:.8g}" for value in values) + "]"


def apply(conn: psycopg.Connection, ev: Event, embed: Embedder) -> str:
    """Apply one event on an autocommit connection.
    Returns "indexed", "deleted" or "skipped" (a duplicate or a late event)."""
    args = {"tenant": ev.tenant_id, "doc": ev.doc_id, "version": ev.version,
            "deleted": ev.type == "delete", "model": embed.name}
    this_doc = "WHERE tenant_id = %(tenant)s AND doc_id = %(doc)s"

    if ev.type == "delete":
        with conn.transaction():
            if conn.execute(CLAIM, args).fetchone() is None:
                return "skipped"
            conn.execute(f"DELETE FROM chunks {this_doc}", args)
        return "deleted"

    # A cheap exit before paying for embeddings. The claim below is what makes it safe.
    seen = conn.execute(f"SELECT version FROM documents {this_doc}", args).fetchone()
    if seen and seen[0] >= ev.version:
        return "skipped"

    pieces = chunk(ev.title, ev.text)
    hashes = [hashlib.sha256(piece.encode()).hexdigest() for piece in pieces]
    # Chunks whose text didn't change keep their vectors: only new text is embedded.
    vectors = dict(conn.execute(
        f"SELECT chunk_hash, embedding::text FROM chunks {this_doc} "
        "AND embed_model = %(model)s", args).fetchall())
    new = {h: piece for h, piece in zip(hashes, pieces) if h not in vectors}
    if new:
        vectors.update(zip(new, map(to_vector, embed(list(new.values())))))

    with conn.transaction():  # the new version appears and the old one goes, together
        if conn.execute(CLAIM, args).fetchone() is None:
            return "skipped"  # a newer event got in while we were embedding
        for position, (h, piece) in enumerate(zip(hashes, pieces)):
            conn.execute(INSERT_CHUNK, (ev.tenant_id, ev.doc_id, ev.version, h, position,
                                        ev.title, ev.url, piece, list(ev.acl), embed.name,
                                        vectors[h]))
        conn.execute(f"DELETE FROM chunks {this_doc} AND version < %(version)s", args)
    return "indexed"


def reconcile(conn: psycopg.Connection, tenant_id: str, list_source: Callable[[], dict[str, int]],
              fetch: Callable[[str], Event], embed: Embedder) -> dict[str, int]:
    """The safety net for events that never arrived. `list_source` returns the source system's
    own {doc_id: version} listing; `fetch` reads one document from it as an upsert event."""
    indexed = dict(conn.execute(
        "SELECT doc_id, version FROM documents WHERE tenant_id = %s AND NOT deleted",
        (tenant_id,)).fetchall())
    # List the source only now: a document created after this listing but before the index
    # read would look deleted, get a tombstone one version up, and its next version would be
    # skipped as stale. In this order it can only look "behind", which a re-index makes right.
    source = list_source()
    behind = [doc_id for doc_id, version in source.items() if indexed.get(doc_id, -1) < version]
    gone = [doc_id for doc_id in indexed if doc_id not in source]
    for doc_id in behind:
        apply(conn, fetch(doc_id), embed)
    for doc_id in gone:  # one past what we hold, so the same guard applies
        apply(conn, Event("delete", tenant_id, doc_id, indexed[doc_id] + 1), embed)
    return {"reindexed": len(behind), "deleted": len(gone)}
