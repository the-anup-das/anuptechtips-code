-- The policy store: one row per version of a clause. `effective` says when that
-- version is in force, so "what did the policy say on 11 November?" is a query.
CREATE EXTENSION IF NOT EXISTS btree_gist;  -- lets a key mix a text column with a range

CREATE TABLE policy_clause (
    clause_id   text      NOT NULL,           -- 'BRV-02'
    version     int       NOT NULL,
    policy_type text      NOT NULL,           -- what the intent router maps a question to
    owner       text      NOT NULL,           -- the team that signs off the wording
    body        text      NOT NULL,
    effective   tstzrange NOT NULL DEFAULT tstzrange(now(), NULL),
    tsv         tsvector  GENERATED ALWAYS AS (to_tsvector('english', body)) STORED,
    -- PostgreSQL 18: two versions of one clause can never be in force at the same moment.
    PRIMARY KEY (clause_id, effective WITHOUT OVERLAPS),
    UNIQUE (clause_id, version)
);
CREATE INDEX policy_clause_tsv ON policy_clause USING gin (tsv);

-- One row per reply: what was asked, what the model drafted, what the customer was told,
-- which clause versions it rested on and what every gate said.
CREATE TABLE answer_audit (
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),
    conversation_id text        NOT NULL,
    question        text        NOT NULL,
    intent          text,
    action          text        NOT NULL CHECK (action IN ('ship', 'handoff')),
    stage           text        NOT NULL,     -- the gate that decided
    draft           text,                     -- the model's words, shipped or not
    reply           text        NOT NULL,     -- the customer's copy
    cited           jsonb       NOT NULL,     -- [["BRV-02", 1]]: clause IDs with versions
    checks          jsonb       NOT NULL,     -- tier-1 problems, tier-2 verdict, cache hit
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- The same reply as an event, written in the same transaction as the audit row.
-- A relay publishes it to Kafka afterwards (relay.py).
CREATE TABLE outbox (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        text        NOT NULL,
    key          text        NOT NULL,        -- the conversation: keeps its events in order
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;
