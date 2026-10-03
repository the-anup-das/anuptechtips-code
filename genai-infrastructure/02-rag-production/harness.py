"""Shared plumbing for the tests and the measurements: a throwaway Postgres schema with the
post's tables, and the small handbook corpus in golden/ as upsert events."""
import json
import pathlib
from urllib.parse import quote

import psycopg

from indexer import Embedder, Event, apply
from retrieval import DSN

HERE = pathlib.Path(__file__).resolve().parent
GOLDEN = HERE / "golden"
TENANT = "acme"


def schema_dsn(schema: str) -> str:
    """The lab DSN with search_path set, so the unqualified table names land in `schema`.
    `public` stays on the path: that is where the vector type lives."""
    return f"{DSN}?options={quote(f'-csearch_path={schema},public')}"


def setup_schema(schema: str) -> str:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.execute(f'CREATE SCHEMA "{schema}"')
    dsn = schema_dsn(schema)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((HERE / "schema.sql").read_text())
    return dsn


def drop_schema(schema: str) -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def corpus_events(version: int = 1) -> list[Event]:
    """One upsert event per handbook page. The first line of each file is its title."""
    manifest = json.loads((GOLDEN / "manifest.json").read_text())
    events = []
    for doc_id, meta in manifest.items():
        title, _, text = (GOLDEN / "corpus" / f"{doc_id}.md").read_text().partition("\n")
        events.append(Event("upsert", TENANT, doc_id, version, title.removeprefix("# ").strip(),
                            f"https://handbook.example/{doc_id}", text, tuple(meta["acl"])))
    return events


def index_corpus(conn: psycopg.Connection, embed: Embedder) -> None:
    for event in corpus_events():
        apply(conn, event, embed)


def orphan_chunks(conn: psycopg.Connection) -> int:
    """Chunks that don't belong to the live version of a live document. Should be 0."""
    return conn.execute(
        "SELECT count(*) FROM chunks c LEFT JOIN documents d USING (tenant_id, doc_id) "
        "WHERE d.doc_id IS NULL OR d.deleted OR c.version <> d.version").fetchone()[0]
