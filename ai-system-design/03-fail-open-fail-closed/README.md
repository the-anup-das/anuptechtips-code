# 03 · Fail open vs fail closed: generated config, kill switches and false 429s

Code, tests and measurements for **Fail Open vs Fail Closed: What Cloudflare and OpenAI Learned
When Generated Config Broke**, part 3 of the "AI System Design Case Studies" series on
anuptechtips.com: https://anuptechtips.com/fail-open-vs-fail-closed/

The lab rebuilds the bug behind Cloudflare's 18 November 2025 outage on PostgreSQL and Kafka,
then builds the design that stops it. There is no model in it: the "proxies" are small Python
objects that score fake requests with whatever feature file they were sent.

- a permission change makes a metadata query return every feature twice (60 rows become 120);
- a naive consumer crashes on the doubled file, another one "fails wrong" (a bot score of 0
  for everyone), and a hardened one keeps serving the last known good file;
- a staged rollout (cohorts of 1, 3 and 8 proxies behind a health gate) stops the file at
  one proxy;
- a kill switch is tested on every action it can touch;
- a rate limiter whose Redis is down answers 503 or falls back to a local bucket, never 429.

## Files

| File | What it is |
|---|---|
| `schema.sql` | `public.http_requests_features` and `r0.http_requests_features` (60 feature columns each), the role `feature_gen` and four "node" roles |
| `generator.py` | The generator's query without and with the schema filter, and the producer's check |
| `feature_file.py` | `bot_score` (the preallocated buffer that overflows) and `validate_feature_file` |
| `config_holder.py` | `ConfigHolder` (last known good, replays ignored, `None` for unknown), plus the naive and the score-zero holders |
| `proxy.py` | A stand-in proxy: reads feature files from its cohort's Kafka topic, serves 100 synthetic requests a second, reports health counters to Redis |
| `rollout.py` | `rollout()` over stages with a health gate and rollback; `STAGED` and `GLOBAL` plans |
| `killswitch.py` | A small rules engine with a kill switch: the naive version and the fixed one |
| `limiter.py` | FastAPI rate limiter: `limit(fail_open)` (503 or a local token bucket) next to a naive version that says 429 |
| `token_bucket.py`, `lua/token_bucket.lua` | The in-process and the Redis token bucket from the rate limiting post |
| `tarpit.py` | Two ways for Redis to be down: a port that refuses connections, and a server that never answers |
| `lab.py` | Connection settings, topic and key names |
| `measure_rows.py` | M1: rows before and after the grant, which privileges matter, the schema filter |
| `measure_consumers.py` | M2: one doubled file, five kinds of consumer, 12,000 requests each |
| `measure_rollout.py` | M3: global push vs staged rollout, 20 runs per variant |
| `measure_rollout_warmup.py` | M3b: the two naive runs of M3 again with six warm-up lengths, to see how much of the timing is the harness |
| `measure_flapping.py` | M4: 30 files generated while four node roles get the grant in turn, then replayed to three fleets; 5 seeds |
| `measure_killswitch.py` | M5: every action, kill switch off and on |
| `measure_limiter.py` | M6: statuses and latency with the limiter's Redis healthy, refusing connections and hung |
| `measure_kafka_family.py` | Plumbing check: one publish over IPv4 and over IPv6, the reason `lab.py` pins the Kafka clients to IPv4 |
| `tests/` | pytest against the Docker services, one test per path the post describes |
| `results/` | raw CSV/JSON from every run |

## Run it

The services are the Docker containers from the repo's `reliable-python-services/` folder
(`docker compose up -d` there): PostgreSQL 18 on port 55432, Redis 8 on 56379 and Kafka 4 on
59092. This folder uses its own Postgres database (`failmodes`, created on first use), Redis
DB 8 and Kafka topics that start with `failmodes.`. It also creates five roles in the Postgres
cluster: `feature_gen` and `feature_node_1` to `feature_node_4`. Override the addresses with
`FAILMODES_DSN`, `FAILMODES_ADMIN_DSN`, `FAILMODES_REDIS_URL` and `FAILMODES_KAFKA`.

```shell
cd ai-system-design/03-fail-open-fail-closed
pytest -q
python measure_rows.py
python measure_consumers.py
python measure_rollout.py
python measure_rollout_warmup.py
python measure_flapping.py
python measure_killswitch.py
python measure_limiter.py
```

`measure_rollout.py` takes about 7 minutes, `measure_rollout_warmup.py` about 5 and
`measure_flapping.py` about 13 (five runs of 150 seconds; a stopped run carries on from its
checkpoint). Set `RUNS=2` for a quick look.
`uvicorn limiter:app --port 8003` serves the rate limiter on its own.

## Two things the lab found out about its own plumbing

- **`localhost` and Kafka on Docker Desktop.** `localhost` means both `::1` and `127.0.0.1`. On
  the test machine, publishing one feature file (887 bytes) took a median of 50 ms over `::1`
  and 0.6 ms over IPv4 (`measure_kafka_family.py`, 500 publishes each). The early runs of the
  rollout tests hit this, so `lab.KAFKA_CLIENT` now pins `broker.address.family` to `v4`, and
  the timings measure the rollout and not the port forwarder.
- **A 50 ms Redis timeout is not a 50 ms failure.** In redis-py 8.1.0,
  `redis.Redis(host, port, socket_timeout=0.05)` keeps the default 5-second connect timeout and
  retries a failed command 10 times with backoff. (`Redis.from_url` builds its pool another way
  and does not retry by default.) `limiter.connect()` sets both timeouts and turns retries off.
  M6 below has the numbers.

## Results

AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2); Python 3.12, PostgreSQL 18.6, Kafka 4.3.1, Redis 8.10.2. Run on 1 and 2 October 2026. Your timings will differ; the counts should not.

### M1: rows the generator's query returns (5 runs, all the same)

| Privilege granted to `feature_gen` on `r0` | Rows, no filter | Distinct names | Rows with the schema filter | Can it read the table? |
|---|---|---|---|---|
| before the change | 60 | 60 | 60 | no |
| USAGE on schema r0 only | 60 | 60 | 60 | no |
| SELECT on r0.http_requests_features | 120 | 60 | 60 | no |
| SELECT on the table and USAGE on the schema | 120 | 60 | 60 | yes |
| INSERT on the table | 120 | 60 | 60 | no |
| UPDATE on the table | 120 | 60 | 60 | no |
| REFERENCES on the table | 120 | 60 | 60 | no |
| DELETE on the table | 60 | 60 | 60 | no |
| TRUNCATE on the table | 60 | 60 | 60 | no |
| TRIGGER on the table | 60 | 60 | 60 | no |

### M2: one doubled file, five kinds of consumer (5 runs, all the same; 12 proxies x 1,000 requests)

| Outcome | 200 | 403 | 500 | 503 | Humans blocked | Bots passed | Served on a stale file |
|---|---|---|---|---|---|---|---|
| good file (baseline) | 10,800 | 1,200 | 0 | 0 | 0 | 0 | 0 |
| crash | 0 | 0 | 12,000 | 0 | 0 | 0 | 0 |
| fail wrong | 0 | 12,000 | 0 | 0 | 10,800 | 0 | 0 |
| fail stale | 10,800 | 1,200 | 0 | 0 | 0 | 0 | 12,000 |
| fail open | 12,000 | 0 | 0 | 0 | 0 | 1,200 | 0 |
| fail closed | 0 | 0 | 0 | 12,000 | 0 | 0 | 0 |

### M3: global push vs staged rollout (20 runs per row; median, min to max)

Both plans use the same health gate and the same automatic rollback. `rollout ms` is the time from the first publish until `rollout()` returned.

| Consumers / plan / file / gate | Stages passed | Proxies with errors | Failed requests | Proxies serving stale | Stale serves | Humans blocked | Rollout ms | Last error ms |
|---|---|---|---|---|---|---|---|---|
| naive / global / bad file / gate: errors and blocks | 0 | 12 | 58 (48 to 79) | 0 | 0 | 0 | 56.3 (55.5 to 58.9) | 46.65 (43.1 to 56.7) |
| naive / staged / bad file / gate: errors and blocks | 0 | 1 | 31 (26 to 31) | 0 | 0 | 0 | 307.35 (256.2 to 312.1) | 302.65 (250.6 to 307.1) |
| hardened / global / bad file / gate: errors and blocks | 0 | 0 | 0 | 12 | 63 (48 to 81) | 0 | 56.6 (55.7 to 60.3) | - |
| hardened / staged / bad file / gate: errors and blocks | 0 | 0 | 0 | 1 | 5 (4 to 7) | 0 | 52.6 (52.1 to 54.1) | - |
| fail wrong / staged / bad file / gate: errors and blocks | 0 | 0 | 0 | 0 | 0 | 27 (23 to 28) | 306.65 (256.4 to 308.8) | - |
| fail wrong / staged / bad file / gate: errors only | 3 | 0 | 0 | 0 | 0 | 2,205.5 (2,192 to 2,223) | 3,059.1 (3,053 to 3,063.8) | - |
| naive / global / good file / gate: errors and blocks | 1 | 0 | 0 | 0 | 0 | 0 | 1,037.8 (1,034.3 to 1,043.5) | - |
| naive / staged / good file / gate: errors and blocks | 3 | 0 | 0 | 0 | 0 | 0 | 3,061.9 (3,054 to 3,069.2) | - |

### M3b: how much of M3's timing is the harness?

The gate looks every 50 ms and the proxies report every 100 ms, and M3 always warms up for 0.5 s, so in M3 the doubled file always lands at the same point of the report cycle. The two naive runs again, 10 runs for each warm-up of 0.5, 0.525, 0.55, 0.575, 1, 2 s:

| Plan | Runs | Proxies with errors | Failed requests | Rollout ms | Runs stopped at the gate's check number | Median rollout ms by warm-up |
|---|---|---|---|---|---|---|
| naive, global | 60 | 12 | 123 (48 to 191) | 107.4 (55.4 to 160.7) | 1: 25, 2: 31, 3: 4 | 0.5 s: 56.4, 0.525 s: 56.2, 0.55 s: 109.8, 0.575 s: 107.8, 1.0 s: 107.7, 2.0 s: 56.6 |
| naive, staged | 60 | 1 | 26 (20 to 31) | 256.2 (204.2 to 309.8) | 4: 18, 5: 20, 6: 22 | 0.5 s: 307.2, 0.525 s: 256, 0.55 s: 256.1, 0.575 s: 205.1, 1.0 s: 204.9, 2.0 s: 307.4 |

So M3's 56 ms for the global push (the gate's first check, in all 20 runs) was the 0.5 s warm-up lining up with the report cycle. Across warm-ups the global push was stopped at the first, second or third check, and always before the earliest staged run. The counts of proxies hit did not move.

### M4: flapping (5 seeded runs of 30 files, one every 5 s; median, min to max)

Doubled files per run: seed 1: 16, seed 2: 12, seed 3: 16, seed 4: 19, seed 5: 15. The naive and the hardened global push have no gate here.

| Fleet | Failed requests | Failed share % | Seconds with all 12 failing | Seconds with any failing | Most proxies failing at once | Stale serves | Oldest file at the end (s) |
|---|---|---|---|---|---|---|---|
| naive, global push | 95,981 (71,979 to 113,972) | 53.32 (39.99 to 63.32) | 80 (60.1 to 95.1) | 80.1 (60.3 to 95.2) | 12 | 0 | - |
| naive, staged rollout | 432 (323 to 492) | 0.24 (0.18 to 0.27) | 0 | 5 (3.9 to 6) | 1 | 0 | - |
| hardened, global push | 0 | 0 | 0 | 0 | 0 | 96,101 (72,090 to 114,114) | 65 (35 to 70) |

### M5: the kill-switch matrix

| Action | Kill switch | Naive engine | Fixed engine |
|---|---|---|---|
| block | off | ok | ok |
| block | on | ok | ok |
| log | off | ok | ok |
| log | on | ok | ok |
| skip | off | ok | ok |
| skip | on | ok | ok |
| execute | off | ok | ok |
| execute | on | AttributeError: 'NoneType' object has no attribute 'results_index' | ok |

### M6: the limiter's Redis is down

First request of a new client:

| Limiter's Redis | Route | Status | Retry-After |
|---|---|---|---|
| healthy | naive limiter | 200 | - |
| healthy | fail open (/search) | 200 | - |
| healthy | fail closed (/login) | 200 | - |
| refusing connections | naive limiter | 429 | 1 |
| refusing connections | fail open (/search) | 200 | - |
| refusing connections | fail closed (/login) | 503 | 1 |
| hung | naive limiter | 429 | 1 |
| hung | fail open (/search) | 200 | - |
| hung | fail closed (/login) | 503 | 1 |

Time for one `POST /login` (redis-py 8.1.0; `redis.Redis(host, port)` defaults: connect timeout 5 s, 10 retries with ExponentialWithJitterBackoff):

| Limiter's Redis / client | Requests | Median ms | Min | Max | p99 |
|---|---|---|---|---|---|
| healthy / both timeouts, no retries | 500 | 3 | 2.33 | 30.78 | 4.49 |
| healthy / both timeouts | 500 | 3.18 | 2.68 | 32.34 | 4.58 |
| healthy / socket_timeout only | 500 | 3.325 | 2.83 | 31.57 | 5.08 |
| refusing connections / both timeouts, no retries | 500 | 61.95 | 58.94 | 103.83 | 64.44 |
| refusing connections / both timeouts | 5 | 4,526.08 | 3,787.91 | 5,383.2 | - |
| refusing connections / socket_timeout only | 5 | 26,602.5 | 26,013.5 | 26,749.1 | - |
| hung / both timeouts, no retries | 500 | 62.235 | 54.18 | 101.25 | 92.51 |
| hung / both timeouts | 5 | 4,780.64 | 4,431.34 | 5,743.28 | - |
| hung / socket_timeout only | 5 | 4,850.54 | 4,211.88 | 5,559.48 | - |

### Plumbing: Kafka over IPv4 and over IPv6

One produce + flush of a 887-byte feature file, 5 rounds of 100 (`measure_kafka_family.py`):

| Address family | Publishes | Median ms | p95 | Min | Max |
|---|---|---|---|---|---|
| v4 | 500 | 0.59 | 0.68 | 0.53 | 0.88 |
| v6 | 500 | 50.01 | 60.02 | 49.76 | 60.21 |
