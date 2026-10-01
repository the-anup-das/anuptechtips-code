# 04 · Idempotent consumer (inbox table) for Kafka, in Python

Code, tests and measurements for the post
**Exactly-Once Is a Myth: Idempotent Consumers in Practice**
(https://anuptechtips.com/idempotent-consumer-pattern/).

Kafka delivers at least once, so a consumer will see some messages twice. This folder shows
the fix: record each message's event ID in a `processed_messages` table **in the same Postgres
transaction as the side effect**, and commit the Kafka offset **after** that transaction.
Then it kills the consumer hard, mid-stream, and counts duplicates and losses with and without it.

## Files

| File | What it is |
|---|---|
| `schema.sql` | the inbox table (`processed_messages`) and the demo side effect (`accounts`, `ledger`) |
| `consume.py` | the consumer shown in the post: confluent-kafka, auto-commit off, one psycopg 3 transaction, synchronous `commit(message=msg)`, dead-letter topic for messages without an event ID |
| `publish.py` | producer side: idempotent producer, event ID in an `event_id` header |
| `retention.sql` | batched cleanup of old inbox rows |
| `worker.py` | runs one variant (a1, a2, b, c) as its own process, so a test can kill it |
| `harness.py` | per-run schema, topic and group plumbing for the measurements |
| `measure_kill.py` | M1: kill the consumer 20× per run, count duplicates and losses |
| `measure_rebalance.py` | M2: a slow consumer evicted by `max.poll.interval.ms` |
| `measure_throughput.py` | M3: msgs/s and per-message gap, with and without the inbox insert (under the measure lock) |
| `measure_storage.py` | M4: bytes per `processed_messages` row, text vs uuid IDs |
| `tests/` | pytest: the inbox logic against Postgres only, plus real-Kafka tests (skipped without a broker) |
| `results/` | raw JSON from every run; `results/logs/` has the consumer logs |

## Variants

| | Offsets | Dedup |
|---|---|---|
| a1 | auto-commit with the defaults, `poll()` one message at a time | none |
| a2 | auto-commit with the defaults, `consume()` 100-message batches | none |
| b | `enable.auto.commit=False`, synchronous commit after the DB commit | none |
| c | same as b (this is `consume.run()`) | inbox row in the same transaction |

In every variant the side effect itself is one Postgres transaction, so a crash can't half-apply it.

## Run it

```sh
docker compose up -d                      # from the series folder, one level up: Postgres 18, Kafka 4.3 (KRaft)
docker compose exec postgres psql -U patterns -c "CREATE DATABASE consumer"
psql postgresql://patterns:patterns@localhost:55432/consumer -f schema.sql
psql postgresql://patterns:patterns@localhost:55432/consumer -c "INSERT INTO accounts (id) SELECT generate_series(1, 100)"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:59092 --create --topic consumer.deposits --partitions 6

pip install "psycopg[binary]" confluent-kafka pytest matplotlib
python -m pytest -q                       # Postgres-only tests run even without Kafka

python publish.py 1000                    # 1,000 demo credits with event_id headers
python consume.py                         # reads consumer.deposits as group wallet-credits
```

Measurements (each writes `results/*.json`):

```sh
python measure_kill.py --variant c --runs 5         # repeat for a1, a2, b
python measure_rebalance.py --variant c --runs 5    # and b
python measure_throughput.py --runs 5
python measure_storage.py --runs 5
```

## Results on the setup above

| | Duplicates per 20-kill run, median (min–max) | Lost per run | msgs/s, no kills (median of 5) |
|---|---|---|---|
| a1 auto-commit, `poll()` | 3,648 (2,777–4,525) | 0 (0–0) | 445.1 |
| a2 auto-commit, 100-msg batches | 2,881 (1,823–3,622) | 63 (48–147) | 447.5 |
| b commit after DB, no inbox | 5 (0–7) | 0 (0–0) | 347.8 |
| c inbox + commit after DB | 0 (0–0) | 0 (0–0) | 323.0 |

- M2 (rebalance zombie, 5 runs): 50 evictions gave 50 duplicates without the inbox, 0 with it.
- M4 (bytes per inbox row, 1M rows): 194.5 with `text` UUIDs, 134.0 with `uuid`, 118.1 with `uuidv7()`.
- `results/summary.json` has the full numbers; `results/logs/m3_b1_discarded.json` is one M3 run
  that overlapped a pytest run and was re-run.

Setup used for the published numbers: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM,
Windows 11, Docker Desktop 28.5 (WSL2); Python 3.12, confluent-kafka 2.15.1 (librdkafka 2.15.1),
psycopg 3.3.6, Kafka 4.3.1 (single node), PostgreSQL 18.6.

On Windows the harness kills the consumer with `Popen.kill()`, which calls `TerminateProcess`:
no `finally` blocks, no `consumer.close()`, no last offset commit.
