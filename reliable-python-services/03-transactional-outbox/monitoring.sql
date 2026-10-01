-- Alert on the age of the oldest unsent event, not on the backlog count:
-- a big backlog that drains in seconds is fine, one row stuck for ten minutes isn't.
SELECT
    coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_pending_seconds,
    count(*)                                                 AS pending,
    count(*) FILTER (WHERE attempts > 0)                     AS retrying
FROM outbox
WHERE published_at IS NULL AND dead_at IS NULL;

-- Dead rows need a human (or a replay script), so page on any.
SELECT count(*) AS dead FROM outbox WHERE dead_at IS NOT NULL;
