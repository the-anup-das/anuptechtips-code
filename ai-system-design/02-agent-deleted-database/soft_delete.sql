-- Run as agentdel_owner. After this, the operator's way to "drop" a production table is
-- ops.soft_drop(): the table moves to the trash schema and stays whole for 48 hours.
CREATE TABLE trash.manifest (
    table_name  text        PRIMARY KEY,
    dropped_by  text        NOT NULL,
    dropped_at  timestamptz NOT NULL DEFAULT now(),
    purge_after timestamptz NOT NULL
);

CREATE FUNCTION ops.soft_drop(tbl text) RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    purge_at timestamptz := now() + interval '48 hours';
BEGIN
    -- A catalog change: the rows, the indexes and the grants all move with the table.
    EXECUTE format('ALTER TABLE prod.%I SET SCHEMA trash', tbl);
    INSERT INTO trash.manifest (table_name, dropped_by, purge_after)
    VALUES (tbl, session_user, purge_at);
    RETURN purge_at;
END $$;

-- Postgres lets PUBLIC execute every new function. Take that away, then name one role.
REVOKE ALL ON FUNCTION ops.soft_drop(text) FROM PUBLIC;
GRANT USAGE ON SCHEMA ops TO agentdel_operator;
GRANT EXECUTE ON FUNCTION ops.soft_drop(text) TO agentdel_operator;

CREATE FUNCTION ops.restore(tbl text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    DELETE FROM trash.manifest WHERE table_name = tbl;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'nothing called % is in the trash', tbl;
    END IF;
    EXECUTE format('ALTER TABLE trash.%I SET SCHEMA prod', tbl);
END $$;

REVOKE ALL ON FUNCTION ops.restore(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.restore(text) TO agentdel_operator;

-- The purge job is the only hard delete left. It is not SECURITY DEFINER, so it works for
-- the owner alone, and only on tables whose 48 hours are over.
CREATE FUNCTION ops.purge() RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    tbl    text;
    purged int := 0;
BEGIN
    FOR tbl IN DELETE FROM trash.manifest WHERE purge_after <= now() RETURNING table_name LOOP
        EXECUTE format('DROP TABLE trash.%I', tbl);
        purged := purged + 1;
    END LOOP;
    RETURN purged;
END $$;

REVOKE ALL ON FUNCTION ops.purge() FROM PUBLIC;
