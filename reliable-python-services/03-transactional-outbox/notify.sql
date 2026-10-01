-- Optional: wake the relay on commit instead of waiting for its next poll.
-- One NOTIFY per inserting statement; Postgres folds identical notifications
-- inside a transaction, and delivers them only if the transaction commits.
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox', '');  -- empty payload: it's a wake-up, not the event
    RETURN NULL;
END $$;

CREATE OR REPLACE TRIGGER outbox_notify
    AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();
