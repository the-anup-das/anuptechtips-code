# Reliable Python services

Part of [anuptechtips-code](../README.md), the code behind the articles on anuptechtips.com.

Runnable code, tests and measurements for the **Reliable Python services** series on
[anuptechtips.com](https://anuptechtips.com/category/system-design/): distributed-systems
patterns built and tested in Python, PostgreSQL, Redis and Kafka.

Every code block in the posts comes from these folders, and every number in the posts comes
from the `measure_*.py` scripts here. The raw results are in each folder's `results/`.
The diagrams and charts are published on the blog only.

| Part | Folder | Post |
|---|---|---|
| 1 | [01-idempotency-keys](01-idempotency-keys/) | [Idempotency Keys: How to Make Retries Safe in Distributed Systems](https://anuptechtips.com/idempotency-keys-distributed-systems/) |
| 2 | [02-rate-limiting](02-rate-limiting/) | [Rate Limiting Algorithms: Token Bucket vs Sliding Window](https://anuptechtips.com/rate-limiting-algorithms/) |
| 3 | [03-transactional-outbox](03-transactional-outbox/) | [Transactional Outbox Pattern in Python (Postgres)](https://anuptechtips.com/transactional-outbox-pattern-python/) |
| 4 | [04-idempotent-consumer](04-idempotent-consumer/) | [Exactly-Once Is a Myth: Idempotent Consumers in Practice](https://anuptechtips.com/idempotent-consumer-pattern/) |
| 5 | [05-redis-locks](05-redis-locks/) | [Redis Distributed Locks in Python: SET NX, Redlock, Fencing Tokens](https://anuptechtips.com/redis-distributed-lock-python/) |

The posts go live one a week from October 2026. Until a post is published, its link returns 404.

## Run it

You need Docker and Python 3.12+. Run these from this folder:

```sh
docker compose up -d        # PostgreSQL 18, Redis 8, Kafka 4.3 (KRaft, single node)
pip install "psycopg[binary]>=3.2" "redis>=8" fastapi httpx confluent-kafka pytest matplotlib numpy
cd 01-idempotency-keys
pytest -q                   # each folder's README lists its tests and measurements
```

The services listen on non-default ports, so they won't clash with anything you already run:

- PostgreSQL: `postgresql://patterns:patterns@localhost:55432/patterns`. Local test credentials only.
- Redis: `localhost:56379`.
- Kafka: `localhost:59092`.

Each folder uses its own database, Redis DB index or topic prefix.

`docker compose down -v` removes the services and their data.

## About the numbers

The posts' measurements ran on one machine:
- AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM;
- Windows 11, Docker Desktop 28.5 (WSL2);
- the services in Docker, Python on the host.

Docker Desktop's network hop dominates some latencies, so your absolute numbers will differ. The comparisons are what matter:
- duplicates vs none;
- lost updates vs none;
- which relay setting is faster.

Timing runs take a cross-process lock (`common/measure_lock.py`), so two benchmarks never overlap.

Found a case that breaks one of these? Open an issue or a pull request.
