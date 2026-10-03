# 02 · An AI agent deleted the production database: the design that stops it

Code, tests and measurements for **AI Agent Deleted the Production Database: 3 Real Cases and
the Design That Stops It**, part 2 of the "AI System Design Case Studies" series on anuptechtips.com:
https://anuptechtips.com/ai-agent-deleted-production-database/

Three public incidents (Replit and SaaStr, DataTalks.Club, PocketOS on Railway) share one
failure chain: the rule lives in the prompt, the credential reaches production, the delete is
immediate, the backups sit in the blast radius, and nothing checks the result. This folder
builds the controls that cut each link outside the model, then replays the three incidents
against them:

- scoped Postgres roles: the agent's role reads production, changes rows in staging and owns nothing;
- a tool gate: unknown calls count as destructive, production is denied by default, and a
  destructive call needs a single-use approval (a Redis key named after the SHA-256 of the call);
- soft delete: an approved `DROP TABLE` moves the table to a trash schema for 48 hours;
- a vault: a read-only role copies production out of the database, and a restore test proves it;
- a token bucket and a kill switch that a Kafka consumer (or you) can throw.

**Nothing here touches a cloud, Terraform or a real volume.** The "agent" is a scripted stand-in
(no model, no API). "terraform destroy" and "volumeDelete" are `DROP TABLE` statements on the
lab's own tables. Everything lives in one Postgres database (`agentdel`), Redis DB 7 and Kafka
topics that start with `agentdel.`. The lab's roles are all named `agentdel_*`, none of them is
a superuser, and the lab refuses to connect to any other database.

## Files

| File | What it is |
|---|---|
| `lab.py` | The database, the five roles (`agentdel_owner`, `_agent`, `_operator`, `_vault`, `_domains`), seed data; `python lab.py --drop` removes the schemas and roles |
| `schema.sql`, `grants.sql` | Four schemas (`prod`, `staging`, `trash`, `ops`) and the least-privilege grants |
| `soft_delete.sql` | `ops.soft_drop()`, `ops.restore()`, `ops.purge()` and the `trash.manifest` table |
| `gate.py` | `ToolGate`: classify, deny production by default, single-use approvals, change freeze, kill switch, token bucket, audit hook |
| `token_bucket.lua` | The bucket the gate uses for destructive calls |
| `executor.py` | Runs what the gate let through: picks the credential, turns `DROP TABLE` into a soft drop, rolls back a statement that touches more rows than its plan |
| `vault.py` | `backup()` as the read-only vault role, `restore()`, `restore_test()` |
| `killswitch.py` | The Kafka watchdog that sets `killswitch:<agent>`, plus `stop` / `clear` from the command line |
| `standin.py` | The scripted stand-in: the three incident replays and the inbox cleanup with a compacting context |
| `replay.py` | The five setups, the `World` the stand-in acts in, and `run()` |
| `measure_*.py` | M1 to M5 (below) |
| `tests/` | pytest against the Docker services, one test per claim in the post |
| `results/` | Raw CSV/JSON from every run |

## Run it

You need Docker and Python 3.12+. Start PostgreSQL 18, Redis 8 and Kafka 4.3 from the
repository's Compose file (`docker compose up -d` in `reliable-python-services/`), then:

```shell
pip install "psycopg[binary]>=3.2" "redis>=8" confluent-kafka pytest
cd ai-system-design/02-agent-deleted-database
pytest -q
python measure_replays.py
python measure_compaction.py
python measure_restore.py
python measure_speedrun.py
python measure_gate_overhead.py
```

The defaults are `postgresql://patterns:patterns@localhost:55432`, Redis on `localhost:56379`
and Kafka on `localhost:59092`; override them with `AGENTDEL_ADMIN_DSN`, `AGENTDEL_REDIS_URL`
and `AGENTDEL_KAFKA`. The admin connection is only used to create the `agentdel` database and
the roles. The roles get derived local test passwords (`AGENTDEL_SECRET` changes them).

`pytest` creates the roles and drops them again when the session ends. The measurement scripts
leave them in place; `python lab.py --drop` removes them. Don't run two of the scripts at the
same time: each one resets the lab's schemas. `measure_replays.py` seeds 1,943,200 rows several
times and takes a few minutes.

## Results

**M1: three replays against five setups, 5 runs, identical every time** (`measure_replays.py`)

| Replay | 1 prompt rule, one credential | 2 + scoped roles | 3 + gate | 4 + soft delete | 5 + vault backups |
|---|---|---|---|---|---|
| Replit (2,402 rows) | 100% lost | refused: `InsufficientPrivilege` | refused: `approval required` | refused by the gate | refused by the gate |
| DataTalks.Club (1,943,200 rows) | 100% lost | 100% lost | 100% lost | 0% lost, restored from the trash | 0% lost, restored from the trash |
| PocketOS (50,000 rows) | 100% lost | refused: `InsufficientPrivilege` | refused by the role | refused by the role | refused by the role |

If nobody restores within the 48 hours, DataTalks.Club setup 4 loses everything and setup 5 restores every row
from the vault. With one leftover owner token on disk, the PocketOS replay is a hard delete in setups 3 to 5,
and only setup 5 (the vault) gets the rows back. The vault restores are a best case: the backup is taken just
before each replay, and a real restore loses everything since the last backup.

Two things in the Replit replay are this lab's choices, not reported facts: the deletion is simulated as a
`DROP TABLE`, and the stand-in then refills both tables with made-up rows (the reports describe fabricated
records as a separate episode). The replay makes 6 calls; from setup 2 on, 5 of them are refused.

**M2: compaction** (`measure_compaction.py`; a deterministic model: 200 emails, the human says no every time)

| Context limit (messages) | Asked first | First unasked delete | Deleted, no gate | Deleted, behind the gate |
|---|---|---|---|---|
| 25 | 8 | email 9 | 192 | 0 |
| 50 | 17 | email 18 | 183 | 0 |
| 100 | 33 | email 34 | 167 | 0 |
| 200 | 67 | email 68 | 133 | 0 |
| 400 | 133 | email 134 | 67 | 0 |
| 1000 | 200 | never | 0 | 0 |

**M3: restore time, median of 20, end to end including a new connection** (`measure_restore.py`)

| Table | From the trash | From the vault | Vault backup |
|---|---|---|---|
| `executives` (1,206 rows) | 29.12 ms | 82.97 ms | 17.68 ms |
| `courses_answer` (1,943,200 rows) | 29.01 ms | 1672.18 ms | 3300.91 ms |

The trash restore statement alone: 3.29 ms and 3.38 ms.

**M4: a 200-delete speedrun, 20 runs** (`measure_speedrun.py`; median, with the range where it varies)

| In the agent's way | Deleted | Refused: rate limit | Refused: kill switch | Switch closed, ms after call 21 began |
|---|---|---|---|---|
| nothing | 200 | 0 | 0 |  |
| token bucket (5, then 1 a minute) | 5 | 195 | 0 |  |
| watchdog, calls back to back | 25 (22 to 45) | 0 | 175 (155 to 178) | 12.2 (5.29 to 70.75) |
| watchdog, 20 ms between calls | 22 (21 to 23) | 0 | 178 (177 to 179) | 46.43 (23.08 to 70.35) |
| bucket and watchdog | 5 | 54 (43 to 64) | 141 (131 to 152) | 32.115 (24.37 to 42.13) |

With nothing in the way, the 200 deletes took a median 326.15 ms.

**M5: what the gate adds to one call, 20 x 1,000 calls, no-op executor** (`measure_gate_overhead.py`)

| Call | Redis round trips | p50 | p99 |
|---|---|---|---|
| Redis PING | 1 | 338.7 µs | 495.5 µs |
| read on prod | 1 | 349.7 µs | 519.0 µs |
| destructive on staging | 2 | 735.0 µs | 1037.2 µs |
| approved write on prod | 3 | 1068.0 µs | 1524.0 µs |
| approved destructive on prod | 4 | 1464.3 µs | 2061.6 µs |
| denied destructive on prod | 4 | 1460.5 µs | 1947.1 µs |
| approved destructive on prod + Kafka audit | 4 | 1482.6 µs | 2017.8 µs |
| approve() (the human's side) | 1 | 367.3 µs | 538.6 µs |

Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2);
Python 3.12, psycopg 3.3.6, redis-py 8.1.0, confluent-kafka 2.15.1, PostgreSQL 18.6, Redis 8.10.2, Kafka 4.3.1.
Your timings will differ; the refusals and the row counts should not.
