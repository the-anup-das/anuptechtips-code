-- One row per (client, key). The primary key is the claim: only one INSERT can win.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    client_id     text        NOT NULL,
    idem_key      text        NOT NULL,
    fingerprint   text        NOT NULL,
    status        text        NOT NULL DEFAULT 'in_progress'
                              CHECK (status IN ('in_progress', 'completed')),
    response_code int,
    response_body jsonb,
    locked_at     timestamptz NOT NULL DEFAULT now(),  -- lease: when the current owner claimed it
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, idem_key)
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at ON idempotency_keys (created_at);

-- The side effect we protect: one row per card charge.
CREATE TABLE IF NOT EXISTS charges (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_id  text        NOT NULL,
    amount     int         NOT NULL,
    currency   text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
