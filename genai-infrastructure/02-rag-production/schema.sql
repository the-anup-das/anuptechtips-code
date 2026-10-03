-- A RAG index in PostgreSQL + pgvector. One row holds a chunk's text, its vector, its keyword
-- entry and its permissions, so a single transaction changes all four together.
CREATE EXTENSION IF NOT EXISTS vector;

-- One row per source document: the newest version the index has applied, or a tombstone.
CREATE TABLE IF NOT EXISTS documents (
    tenant_id  text        NOT NULL,
    doc_id     text        NOT NULL,
    version    bigint      NOT NULL,                -- only ever goes up, deletes included
    deleted    boolean     NOT NULL DEFAULT false,  -- tombstone: a late event can't revive it
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, doc_id)
);

CREATE TABLE IF NOT EXISTS chunks (
    tenant_id   text        NOT NULL,
    doc_id      text        NOT NULL,
    version     bigint      NOT NULL,
    chunk_hash  text        NOT NULL,               -- sha256 of the text that was embedded
    position    int         NOT NULL,
    title       text        NOT NULL,
    url         text        NOT NULL,
    content     text        NOT NULL,               -- title / section path: text
    tsv         tsvector    GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
    acl         text[]      NOT NULL,               -- groups allowed to read this chunk
    embed_model text        NOT NULL,               -- vectors from two models never mix
    embedding   vector(384) NOT NULL,
    PRIMARY KEY (tenant_id, doc_id, version, chunk_hash)
);
CREATE INDEX IF NOT EXISTS chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS chunks_tsv ON chunks USING gin (tsv);
CREATE INDEX IF NOT EXISTS chunks_acl ON chunks USING gin (acl);
