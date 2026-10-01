-- Transactional outbox on PostgreSQL 18.
-- Column names follow Debezium's Outbox Event Router defaults
-- (id, aggregatetype, aggregateid, type, payload), so a later move to CDC
-- is a connector config change, not a migration.

CREATE TABLE orders (
    id          uuid        PRIMARY KEY DEFAULT uuidv7(),
    customer_id text        NOT NULL,
    total_cents integer     NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id              uuid        NOT NULL DEFAULT uuidv7(),  -- time-ordered, built into PG 18
    aggregatetype   text        NOT NULL,                   -- becomes the topic: outbox.event.<aggregatetype>
    aggregateid     text        NOT NULL,                   -- becomes the message key
    type            text        NOT NULL,
    payload         jsonb       NOT NULL,
    txid            xid8        NOT NULL DEFAULT pg_current_xact_id(),  -- see "commit-order gap"
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    dead_at         timestamptz,                            -- set after too many failed attempts
    PRIMARY KEY (id, created_at)                            -- must include the partition key
) PARTITION BY RANGE (created_at);

-- Only rows the relay still has to send live in this index, so it stays small
-- however many published rows the partitions keep for replay.
CREATE INDEX outbox_pending ON outbox (next_attempt_at)
    WHERE published_at IS NULL AND dead_at IS NULL;

-- Rows that failed too often, copied here before their partition is dropped.
CREATE TABLE outbox_dead (LIKE outbox);
