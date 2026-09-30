-- Once: lets the cleanup find old rows without scanning the whole table.
CREATE INDEX IF NOT EXISTS processed_messages_processed_at ON processed_messages (processed_at);

-- From cron, repeated until it deletes 0 rows. Small batches keep locks and WAL bursts short.
DELETE FROM processed_messages
WHERE ctid IN (
    SELECT ctid FROM processed_messages
    WHERE processed_at < now() - interval '14 days'
    LIMIT 10000
);
