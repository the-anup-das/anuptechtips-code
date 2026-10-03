"""Two ingestion consumers that look fine in a demo. Kept here so the delete drill can show
what they cost. Don't ship them."""
import hashlib

import psycopg

from chunking import chunk
from indexer import INSERT_CHUNK, Embedder, Event, to_vector


def _insert_chunks(conn: psycopg.Connection, ev: Event, embed: Embedder) -> None:
    pieces = chunk(ev.title, ev.text)
    for position, (piece, vector) in enumerate(zip(pieces, embed(pieces))):
        conn.execute(INSERT_CHUNK, (
            ev.tenant_id, ev.doc_id, ev.version, hashlib.sha256(piece.encode()).hexdigest(),
            position, ev.title, ev.url, piece, list(ev.acl), embed.name, to_vector(vector)))


def append_only(conn: psycopg.Connection, ev: Event, embed: Embedder) -> str:
    """The demo script grown up: every upsert adds chunks, and nothing ever removes one."""
    if ev.type == "delete":
        return "skipped"  # the script never heard of deletes
    with conn.transaction():
        _insert_chunks(conn, ev, embed)
    return "indexed"


def last_event_wins(conn: psycopg.Connection, ev: Event, embed: Embedder) -> str:
    """Replaces a document's chunks and handles deletes, but trusts the arrival order:
    whichever event comes last wins, even when it is an old one."""
    with conn.transaction():
        conn.execute("DELETE FROM chunks WHERE tenant_id = %s AND doc_id = %s",
                     (ev.tenant_id, ev.doc_id))
        if ev.type == "delete":
            return "deleted"
        _insert_chunks(conn, ev, embed)
    return "indexed"
