# 01 · Idempotency keys in Python (FastAPI, PostgreSQL, Redis)

Code, tests and measurements for **Idempotency Keys: How to Make Retries Safe in Distributed
Systems**, part 1 of the "Reliable Python services" series on anuptechtips.com:
https://anuptechtips.com/idempotency-keys-distributed-systems/

It shows how to make a retried `POST /charges` run once:

- the client makes one `Idempotency-Key` per operation and reuses it on every retry;
- the server claims the key atomically (`INSERT … ON CONFLICT` in Postgres, or `SET NX` in Redis),
  runs the charge, stores the response beside it and replays it to every retry;
- `400` for a missing key, `409` while the first attempt is still running, `422` for the same key
  with a different body, and `Idempotent-Replayed: true` on replays;
- a 30-second lease, so a retry can take over from a worker that crashed mid-charge;
- a race test showing what check-then-insert costs.

## Files

| File | What it is |
|---|---|
| `schema.sql` | `idempotency_keys` (with the `locked_at` lease) and the `charges` side-effect table |
| `idempotency.py` | `fingerprint`, `run_once` (the Postgres claim, replay and lease) and `purge_expired` |
| `app.py` | FastAPI `POST /charges`: header parsing, `run_once`, the replay header |
| `client.py` | `new_key`, `key_for` and `post_with_retries` (httpx) |
| `redis_claim.py` | `run_once_redis`: `SET NX GET` claim plus a Lua compare-and-set |
| `naive.py` | check-then-insert, the version that races (for the race test only) |
| `race_app.py` | benchmark app: the three strategies behind one FastAPI app |
| `measure_race.py` | M1: 50 and 100 concurrent same-key POSTs, 20 runs, count charges and 409s |
| `measure_claim_cost.py` | M2: claim latency p50/p99 and bytes per key, Postgres vs Redis |
| `measure_claim_pileup.py` | M1c: 99 duplicate claims at once, the post's `DO UPDATE … WHERE` claim vs `DO NOTHING` |
| `tests/` | pytest against the Docker services, one test per path the post describes |
| `results/` | raw CSV/JSON from every run |

## Run it

Start the services from the repo root (`docker compose up -d`, see `../docker-compose.yml`). The code
uses Postgres database `idem` (create it once: `CREATE DATABASE idem;`) and Redis DB 1. Override
with `IDEM_DSN` and `IDEM_REDIS_URL`.

```shell
cd 01-idempotency-keys
pytest -q
python measure_race.py
python measure_claim_cost.py
python measure_claim_pileup.py
```

`uvicorn app:app --port 8000` serves the endpoint on its own; send it
`X-Client-Id` and `Idempotency-Key` headers.

## Results

**M1: same-key POSTs sent at once, 20 runs each** (median, with min–max where they differ;
one uvicorn process, 200 AnyIO worker threads, 50 ms of work per charge)

| Strategy | N | Charges | 409s | Replayed 201s | 500s |
|---|---|---|---|---|---|
| check, then insert | 50 | 50 | 0 | 0 | 49 |
| check, then insert | 100 | 100 | 0 | 0 | 99 |
| Postgres `INSERT … ON CONFLICT` | 50 | 1 | 49 (0–49) | 0 (0–49) | 0 |
| Postgres `INSERT … ON CONFLICT` | 100 | 1 | 98 (0–99) | 1 (0–99) | 0 |
| Redis `SET NX` | 50 | 1 | 49 | 0 | 0 |
| Redis `SET NX` | 100 | 1 | 99 | 0 | 0 |

With FastAPI's default 40 worker threads (`--threads 40`), check-then-insert charged 40 of 100 and 42.5 (40–45) of 50; the atomic claims still charged once.

**M1c: 99 duplicate claims at a key already in progress, 20 rounds:** the last one returned after a median 137.53 ms with the post's `ON CONFLICT DO UPDATE … WHERE` claim, and 16.475 ms with `DO NOTHING` (the `DO UPDATE` branch locks the conflicting row even when its `WHERE` is false).

**M2: claim cost, one call at a time over localhost, 20 × 1,000 calls**

| | Postgres p50 / p99 | Redis p50 / p99 |
|---|---|---|
| new-key claim | 1189 / 2012.01 µs | 331 / 470 µs |
| repeat (a retry) | 1526 / 2275.02 µs | 331 / 465.01 µs |

Bytes per completed key (100,000 keys): Postgres 351.3 (heap 248.3 + indexes 102.6), Redis 275 (`MEMORY USAGE`, median of 1,000 keys).

Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2);
Python 3.12, FastAPI 0.142, psycopg 3.3.6, redis-py 8.1.0, PostgreSQL 18.6, Redis 8.10.2.
Your numbers will differ; the counts of charges for the atomic claims should not.
