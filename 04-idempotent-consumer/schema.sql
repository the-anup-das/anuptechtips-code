-- The inbox: one row per message this consumer has fully processed.
CREATE TABLE IF NOT EXISTS processed_messages (
    consumer     text        NOT NULL,              -- which consumer (group) processed it
    message_id   text        NOT NULL,              -- the producer's event ID, not the offset
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- The demo side effect: crediting an account. An increment, so doing it twice is wrong.
CREATE TABLE IF NOT EXISTS accounts (
    id      bigint PRIMARY KEY,
    balance bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ledger (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id   text        NOT NULL,                -- not unique on purpose: the tests count duplicates here
    account_id bigint      NOT NULL REFERENCES accounts (id),
    amount     bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
