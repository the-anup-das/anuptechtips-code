# 03 · Transactional outbox in Python (Postgres)

Code, tests and measurements for the post
[Transactional Outbox Pattern in Python (Postgres)](https://anuptechtips.com/transactional-outbox-pattern-python/)
(Part 3 of the "Reliable Python services" series).

What it shows:

- **The write path** (`write_path.py`): SQLAlchemy 2.0, the order row and its event row in one
  `Session.begin()` block. The request path never talks to the broker.
- **A relay that's safe on several workers** (`relay.py`): claim and mark a batch in one
  `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)`, publish, wait for acks, commit.
  Per-event failures back off (`attempts`, `next_attempt_at`) and go dead after 10 tries; a
  broker outage rolls the whole batch back without burning attempts. `KafkaPublisher` keys by
  `aggregateid` and uses Debezium's topic naming (`outbox.event.<aggregatetype>`).
- **The commit-order gap** (`watermark.py`, `tests/test_ordering.py`): a reader that keeps
  `id > last_seen` loses a row that commits late; storing `txid xid8` and reading only below
  `pg_snapshot_xmin(pg_current_snapshot())` fixes it.
- **LISTEN/NOTIFY as a wake-up** (`notify.sql`, `wakeup.py`), with a drain on start and a
  fallback poll.
- **Cleanup by dropping daily partitions** (`cleanup.py`), which refuses to drop a day that
  still has unpublished rows and copies dead rows to `outbox_dead` first. `monitoring.sql` has
  the "age of the oldest pending row" query.
- `django_outbox.py`: the Django + Celery version. Syntax-checked only (Django and Celery
  aren't installed here).

## Run it

Needs the services from `../docker-compose.yml` (PostgreSQL 18, Kafka 4) and Python 3.12 with
psycopg 3, SQLAlchemy 2.0, confluent-kafka and pytest.

```sh
docker compose -f ../docker-compose.yml up -d
python db.py                 # creates database "outbox", tables, a week of partitions
python -m pytest -q          # 36 tests; they drop and recreate the tables
```

Connection settings can be overridden with `OUTBOX_DSN`, `OUTBOX_ADMIN_DSN` and `OUTBOX_KAFKA`.

## Measurements

Each script writes raw results to `results/`.
Timing runs take `common.measure_lock` so benchmarks from different folders never overlap.

| Script | What it measures |
|---|---|
| `measure_throughput.py` | M1: relay throughput, 1/2/4/8 worker processes × batch 10/100/500, 100,000 rows, stub publisher (~2 ms per batch); checks 0 missing / 0 duplicates |
| `measure_crash.py` | M1: kill -9 a random worker 10 times per run (4 workers, Kafka), then count duplicates in the topic |
| `measure_latency.py` | M2: commit-to-publish latency at 200 commits/s: 1 s polling, 100 ms polling, LISTEN/NOTIFY |
| `measure_notify_overhead.py` | M2: writer commits/s with and without the NOTIFY trigger, 1/8/32 writer processes |
| `measure_cleanup.py` | M3: 1,000,000 published rows: DELETE + VACUUM vs dropping the partition |
| `measure_naive.py` | two copies of the tutorial relay vs two SKIP LOCKED workers, 10,000 rows |
| `diag_claim_cost.py` | where a batch's time goes (claim / publish / commit) as workers are added |
| `measure_message_size.py` | the Docker Desktop message-size quirk below |

Tested on an AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11,
Docker Desktop 28.5 (WSL2), Python 3.12.4, PostgreSQL 18.6, Kafka 4.3.1.

## Results on this machine (medians)

- **Throughput** (events/s, 1/2/4/8 workers): batch 10: 1,801 / 3,630 / 6,912 / 12,883;
  batch 100: 15,390 / 29,355 / 51,619 / 45,143;
  batch 500: 44,759 / 63,139 / 58,492 / 38,003. 0 missing, 0 duplicates in 60 runs.
- **kill -9** (4 workers, Kafka): 50 kills, 2,800 duplicates, 0 missing.
- **Two tutorial relays**: 10,000 of 10,000 events sent twice; two SKIP LOCKED workers: 0.
- **Latency at 200 commits/s** (p50 / p99): 1 s poll 512 / 998 ms; 100 ms poll 55 / 106 ms; LISTEN/NOTIFY 2.9 / 22 ms.
- **NOTIFY trigger cost** (commits/s without / with): 1 writer 435 / 426; 8 writers 2,398 / 940; 32 writers 6,049 / 927.
- **Cleanup, 1,000,000 rows, a long transaction open**: claim 68 ms before, 67 ms after DELETE, 52 ms after VACUUM, 2.2 ms after dropping the partition.

**Docker Desktop on Windows quirk:** any single message larger than about 8 KB that the client
sends to Postgres through the published port costs an extra ~50 ms (measured: 8,000 bytes 0.35 ms, 9,000 bytes 50.01 ms; the same queries over TCP inside the
container took under 0.5 ms).
The relay avoids it by claiming and marking in one statement, so it never sends a list of ids
back. Keep it in mind before benchmarking anything else on this setup.
