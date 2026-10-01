# 02 · Rate limiting algorithms, measured

Code for the anuptechtips.com post **Rate Limiting Algorithms: Token Bucket vs Sliding Window**
(Part 2 of the Reliable Python services series): https://anuptechtips.com/rate-limiting-algorithms/

The same limit, 100 requests a minute, and the same traffic go through all six algorithms:
fixed window, sliding window log, sliding window counter, token bucket, leaky bucket and GCRA.

| File | What it is |
|---|---|
| `simulate.py` | The six algorithms as pure-Python simulations (integer ms, exact arithmetic), plus the traffic patterns |
| `token_bucket.py` | The small thread-safe, in-process `TokenBucket` from the post (with a `clock` parameter for tests) |
| `lua/*.lua` | The six limiters as atomic Redis Lua scripts. They use Redis's clock and return milliseconds |
| `redis_limiters.py` | Registers the scripts (EVALSHA), builds the same six as a Redis Functions library (FCALL), and holds the racy and MULTI/EXEC fixed windows the post compares them with |
| `app.py` | A FastAPI dependency: 429 + `Retry-After` + draft-11 `RateLimit` headers, and fail open or closed per route |
| `measure_edge_burst.py` | M1 + M3: the window-edge burst, the most allowed in any 60 s, and GCRA vs token bucket decision by decision |
| `measure_counter_error.py` | M2: the sliding window counter against the exact log, 10,000 keys, uniform and bursty traffic |
| `measure_redis_memory.py` | M4: `MEMORY USAGE` per key for each algorithm (runs under the measure lock) |
| `measure_redis_latency.py` | M5: p50/p99 per decision for the racy version, MULTI/EXEC, EVALSHA and FCALL (runs under the measure lock) |
| `measure_race.py` | The race in numbers: 20 threads, 500 requests, a limit of 100 |

## Run it

From the series folder, one level up, start the services once:

```sh
docker compose up -d          # Redis 8 on localhost:56379 (this folder uses DB 2)
pip install redis fastapi httpx pytest matplotlib numpy
```

Then, in this folder:

```sh
pytest -q                     # simulator, token bucket, Lua/Functions against Redis, FastAPI
python measure_edge_burst.py
python measure_counter_error.py
python measure_redis_memory.py
python measure_redis_latency.py
python measure_race.py
uvicorn app:app --port 8002   # try it: curl -i localhost:8002/search -H "x-api-key: me"
```

Tested with Python 3.12, redis-py 8.1.0, fastapi 0.142.2 and Redis 8.10.2 in Docker, on an
AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2).
The simulations are deterministic, so any machine gives the same counts. The Redis latencies
are mostly Docker Desktop's network round trip, so yours will differ.

## What the runs found

Same 100/min limit. Traffic: 200 requests across a minute boundary, then 3x the limit for four minutes.

| Algorithm | Most allowed in any 1 s | Most allowed in any 60 s |
|---|---|---|
| Fixed window | 200 | 200 |
| Sliding window log | 100 | 100 |
| Sliding window counter | 100 | 199 |
| Token bucket, B = 20 | 21 | 119 |
| Token bucket, B = 100 | 101 | 199 |
| GCRA, B = 20 | 21 | 119 |
| Leaky bucket, queue of 20 | 2 (reaching the backend) | 100 (up to 12 s of waiting) |

The full numbers are in `results/`. Found a traffic pattern that breaks one of these differently?
Add it to `simulate.PATTERNS` and send a PR.
