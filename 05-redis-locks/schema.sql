-- The protected resource: one counter row per id.
-- fence = the highest fencing token that has claimed or written this row.
CREATE TABLE IF NOT EXISTS counters (
    id    int    PRIMARY KEY,
    value bigint NOT NULL DEFAULT 0,
    fence bigint NOT NULL DEFAULT 0
);
