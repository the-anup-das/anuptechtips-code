-- The lab's stand-in for the database behind Cloudflare's feature file: the table the
-- generator is meant to read (in public), and an underlying table with the same name in
-- schema r0. One column per ML feature, 60 in each.
CREATE SCHEMA IF NOT EXISTS r0;

DO $$
DECLARE
    cols text;
    role text;
BEGIN
    SELECT string_agg(format('feature_%s double precision', to_char(i, 'FM00')), ', ' ORDER BY i)
      INTO cols FROM generate_series(1, 60) AS i;
    EXECUTE format('CREATE TABLE IF NOT EXISTS public.http_requests_features (%s)', cols);
    EXECUTE format('CREATE TABLE IF NOT EXISTS r0.http_requests_features (%s)', cols);

    -- feature_gen runs the generator's query. The four node roles stand for four database
    -- nodes that receive the permission change one at a time (the flapping test).
    FOREACH role IN ARRAY ARRAY['feature_gen', 'feature_node_1', 'feature_node_2',
                                'feature_node_3', 'feature_node_4'] LOOP
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', role);
        END IF;
        EXECUTE format('GRANT SELECT ON public.http_requests_features TO %I', role);
        -- Start every run from "before the change": no privilege of any kind on r0.
        EXECUTE format('REVOKE ALL ON r0.http_requests_features FROM %I', role);
        EXECUTE format('REVOKE ALL ON SCHEMA r0 FROM %I', role);
    END LOOP;
END $$;
