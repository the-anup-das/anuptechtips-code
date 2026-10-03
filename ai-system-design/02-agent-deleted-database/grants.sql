-- Run as agentdel_owner. It owns every table, and no agent ever logs in as it.

-- The agent's credential: production is read-only, staging is its workspace.
GRANT USAGE ON SCHEMA prod, staging TO agentdel_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA prod TO agentdel_agent;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO agentdel_agent;
-- GRANT ... ON ALL TABLES covers today's tables only. These cover the ones created later.
ALTER DEFAULT PRIVILEGES IN SCHEMA prod GRANT SELECT ON TABLES TO agentdel_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO agentdel_agent;
-- It owns nothing and has no CREATE or TRUNCATE anywhere, so DROP, TRUNCATE and ALTER
-- fail in both schemas, and so does every write to prod.

-- The operator's credential, used for approved production changes: rows, never DDL.
GRANT USAGE ON SCHEMA prod TO agentdel_operator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prod TO agentdel_operator;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO agentdel_operator;

-- The backup job reads production and writes nothing.
GRANT USAGE ON SCHEMA prod TO agentdel_vault;
GRANT SELECT ON ALL TABLES IN SCHEMA prod TO agentdel_vault;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod GRANT SELECT ON TABLES TO agentdel_vault;

-- A token minted for one job (custom domains) can do that job and nothing else.
GRANT USAGE ON SCHEMA prod TO agentdel_domains;
GRANT SELECT, INSERT, DELETE ON prod.domains TO agentdel_domains;
