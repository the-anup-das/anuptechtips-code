-- Run as agentdel_owner, the role that owns everything in the lab.
-- Four schemas stand in for four places.
CREATE SCHEMA prod;      -- production: the tables the incidents deleted
CREATE SCHEMA staging;   -- the agent's workspace
CREATE SCHEMA trash;     -- where a "dropped" production table waits before it is purged
CREATE SCHEMA ops;       -- the only delete and restore paths anyone but the owner gets
