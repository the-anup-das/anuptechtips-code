# 05 · Redis distributed locks in Python: SET NX, Redlock, fencing tokens

Code, tests and measurements for the anuptechtips.com post
**Redis Distributed Locks in Python: SET NX, Redlock, Fencing Tokens**
(https://anuptechtips.com/redis-distributed-lock-python/).

A Redis lock is a timed lease, not a guarantee. This folder breaks one with a stall
that outlives the lease, then makes Postgres reject the stale writer with a fencing token.

## What's here

| File | What it shows | Post section |
|---|---|---|
| `simple_lock.py` | `SET key token NX PX ttl` plus a compare-and-delete release (`DELEX … IFEQ` on Redis 8.4+, a Lua script before that) | How does a Redis distributed lock work? |
| `redispy_lock.py` | redis-py's `Lock` with `timeout` and `blocking_timeout` set, sync and `redis.asyncio` | How do you use redis-py's Lock? |
| `break_it.py` | Two workers, one lock, a 3 s stall on a 2 s lease: the counter ends at 1, not 2 | What happens if the lock expires…? |
| `renewing.py` | A background renewer (thread and asyncio) that tells the work when the lease is lost | Can you renew a Redis lock? |
| `fencing.py` | `FencedLock` (redis-py `Lock` + a token from one Lua script), `fenced_write()`, `claim_then_write()` | What is a fencing token? |
| `fence_it.py` | `break_it.py` with fencing, plus the "stale write inside the next holder's read-write gap" overlap | What is a fencing token? |
| `advisory.py` | `pg_try_advisory_xact_lock` with a stable blake2b key | Redis lock vs Postgres advisory lock… |
| `schema.sql` | The `counters` table (`value`, `fence`) | |
| `measure_fencing.py` | Measurement 1: lost updates with no fencing, write-only fencing and claim-first fencing | |
| `measure_polling.py` | Measurement 3: the extra wait from redis-py's `sleep=0.1` polling | |
| `measure_latency.py` | Measurement 2, single-instance part: acquire + release round trip | |

## Run it

Start the services from the series folder, one level up (`docker compose up -d`): Redis 8.10 on `localhost:56379`
and PostgreSQL 18.6 on `localhost:55432`. This folder uses Redis DB 5 and a Postgres database
called `locks` (`CREATE DATABASE locks;`). Override with `REDIS_URL` and `PG_DSN`.

```sh
pip install "redis==8.1.0" "psycopg[binary]==3.3.6" pytest matplotlib
cd 05-redis-locks
python break_it.py                      # final value: 1
python fence_it.py pause write-only     # final value: 2
python fence_it.py overlap write-only   # final value: 1  (the overlap write-only fencing misses)
python fence_it.py overlap claim-first  # final value: 2
python -m pytest -q
```

The tests freeze a real process to show that renewal can't save a paused worker: SIGSTOP on
Linux/macOS, `NtSuspendProcess` on Windows (`tests/procfreeze.py`). DELEX needs Redis 8.4+;
the Lua fallback is tested on the same server with `use_delex=False`.

## Measurements

```sh
python measure_fencing.py        # ~35 min: 5 runs x 3 stall rates x 3 modes x 10,000 sections
python measure_fencing.py --work 0.1 --stall-min 0.1 --rates 0.05 --sections 2000 \
       --out results/fencing_overlap.csv          # Hochstein's overlap, ~20 min
python measure_polling.py        # ~10 min, takes ../.measure.lock
python measure_latency.py        # ~1 min, takes ../.measure.lock
```

If a grid run is interrupted, resume it without redoing finished runs, e.g.
`python measure_fencing.py --first-run 5 --runs 1 --rates 0.05 --append` (the run number is also the seed).

`measure_fencing.py` scales the post's story down 10x so the grid finishes in well under an hour:
TTL 200 ms, stalls 300 ms (1.5x the TTL), 5 ms of work, 10 ms lock polling, 8 worker processes.
Raw per-attempt event logs go to `results/events/` (or `--events DIR`); they are not committed.

Results in the post came from an AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11,
Docker Desktop 28.5 (WSL2), Python 3.12.4, redis-py 8.1.0, psycopg 3.3.6. Yours will differ in the
details; the shape shouldn't.

Only one Redis instance runs here, so Redlock across 3 or 5 nodes is not measured.
