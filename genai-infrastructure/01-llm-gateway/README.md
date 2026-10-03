# 01 · A minimal LLM gateway in Python (FastAPI, httpx, Redis)

Code, tests and measurements for the post
[LLM Gateway Architecture: Routing, Caching, Fallbacks and Budgets](https://anuptechtips.com/llm-gateway-architecture/).

It is one OpenAI-compatible endpoint, `POST /v1/chat/completions`, in front of several model
providers, built to show what a gateway does when something fails:

- **Virtual keys and routes** (`config.py`, `routes.py`): callers hold a key the gateway issued and
  name a task (`support-reply`), never a model ID. The route table maps the task to an ordered chain
  of provider and model, each with its own timeouts and `max_tokens` cap.
- **Errors that mean different things** (`errors.py`): retry a 429 or a 5xx on the same provider,
  fail over at once on `slow_down`, a timeout or a refused key, stop on a spend-limit error, and
  hand a plain 400 back to the caller.
- **A circuit breaker shared through Redis** (`breaker.py`): closed, open, half-open; the open time
  comes from `Retry-After`; one probe at a time across all replicas.
- **Streaming** (`upstream.py`): a first-token timeout (fail over while the caller has seen
  nothing), an idle-gap timeout, an in-band error event after the first token, and usage settled
  on the tokens that were actually sent.
- **Per-tenant budgets** (`budget.py`, `lua/`): reserve the priced worst case in one Lua script,
  across org, team, tenant and key, then settle the real cost. `naive_budget.py` is the
  check-then-spend version that races, kept for the race test.
- **Backpressure** (`admit.py`): in-flight caps per tenant (429) and per provider (fail over, or
  503 with `Retry-After`).
- **Idempotency-Key** (`idempotency.py`): a client's retry gets a 409 while the first attempt
  runs and the stored answer afterwards.
- **An exact-match cache** (`cache.py`) and **usage records as OpenTelemetry spans**
  (`telemetry.py`, GenAI conventions pinned to a commit).

No model, no provider account and no bill: `mock_upstream.py` plays every provider and can
answer 503 or 429, hang, stall or drop a stream on command. It does not have guardrails or
redaction, an admin UI, key rotation or a durable billing ledger.

## Files

| File | What it is |
|---|---|
| `gateway.py` | the request path: authenticate, policy, cache, the fallback loop, streaming, settle and record |
| `upstream.py` | one call to one provider: `complete`, `open_stream` (up to the first token) and `relay` |
| `routes.py` | `Target`, `Route`, the route table and the price table (micro-dollars) |
| `config.py` | virtual keys, provider credentials and `Settings` |
| `errors.py` | `classify`: an upstream response becomes retry, fail over, stop or fail |
| `breaker.py` | the Redis circuit breaker and its half-open probe lock |
| `budget.py`, `lua/reserve.lua`, `lua/settle.lua` | reserve, then settle |
| `naive_budget.py` | check, then spend (for the race test only) |
| `admit.py` | in-flight limits and the bounded queue |
| `idempotency.py` | `claim`: `SET NX GET` and the 409 / 422 / replay rules |
| `cache.py`, `telemetry.py`, `tokens.py` | cache key, usage records and spans, token counting for the mock |
| `mock_upstream.py` | the mock provider with failure switches (`POST /_control`) |
| `concept.py` | the idea in 60 lines: the synchronous sketch the article started from |
| `harness.py` | runs the apps on free ports for the tests and the measurements |
| `measure_latency.py` | M1: added latency, direct vs proxy vs every check, at 1, 16 and 64 callers |
| `measure_burst.py` | M1b: 64 requests in the same instant, through an empty and a full connection pool |
| `measure_pool_burst.py` | M1c: the same burst with httpx alone, to see what the HTTP client costs |
| `measure_failover.py` | M2: a 15-second outage with and without the breaker and the first-token timeout |
| `measure_retry_stacking.py` | M3: upstream calls for one click when app, SDK and gateway all retry |
| `measure_budget_race.py` | M4: 200 requests at once on a budget for 10 |
| `tests/` | pytest over real HTTP against the mock and Redis, one test per path the post describes |
| `results/` | raw CSV/JSON from every run |

## Run it

You need Python 3.12 with `fastapi`, `uvicorn`, `httpx`, `redis` and `pytest` (the retry-stacking
test and M3 also use the `openai` SDK, and the span tests `opentelemetry-sdk`), and a Redis on
`localhost:56379`: `docker compose up -d` in `../../reliable-python-services/`. The code uses
Redis DB 11 and flushes it between tests. Override with `LLMGW_REDIS_URL`.

```shell
cd genai-infrastructure/01-llm-gateway
pytest -q
uvicorn mock_upstream:app --port 8001 &
uvicorn gateway:app --port 8000 &
curl -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer vk-demo-refunds-acme" \
  -d '{"model": "support-reply", "messages": [{"role": "user", "content": "hello"}]}'
```

Break a provider and ask again; the response headers say who answered (`x-llmgw-target`,
`x-llmgw-hop`, `x-llmgw-attempts`):

```shell
curl -s localhost:8001/_control -d '{"provider": "alpha", "mode": "status", "error": "overloaded_503"}'
```

The measurements start their own servers on free ports:

```shell
python measure_retry_stacking.py
python measure_budget_race.py
python measure_failover.py --runs 10
python measure_latency.py --rounds 10
python measure_burst.py
python measure_pool_burst.py
```

## Results

**M3: one click against a provider that always answers 503, 20 runs** (upstream calls counted by the mock; median, min-max)

| Who retries | Upstream calls | Seconds until the click fails |
|---|---|---|
| app 3 tries, SDK defaults, gateway 3 attempts | 27 | 11.375 (7.79-13.25) |
| app 3 tries, SDK `max_retries=0`, gateway 3 attempts | 9 | 2.48 (1.46-3.89) |
| app 1 try, SDK `max_retries=0`, gateway 3 attempts | 3 | 1.03 (0.42-1.71) |
| app 3 tries, SDK defaults, gateway answers `x-should-retry: false` | 9 | 2.835 (1.78-3.6) |

**M4: 200 requests at once on a budget for 10, 20 runs** (the mock takes 2 s per call)

| Budget | Served (200) | Refused (429) | Other (500) | Spend, times the budget |
|---|---|---|---|---|
| check, then spend (`naive_budget.py`) | 200 (192-200) | 0 | 0 (0-8) | 20 (19.2-20) |
| reserve, then settle (`budget.py`) | 10 | 190 | 0 | 1 |

The 500s (36 requests in 6 of the runs) are check-then-spend requests that waited more than 2 seconds for a pooled Redis connection during the check and never reached the model.

**M2: a 15-second outage of the primary, 10 runs** (60 streamed requests per run, one every 250 ms; attempt timeout 5 s, first-token timeout 1 s, breaker opens 5 s after 3 failures; the fallback answers in 50 ms; medians of the per-run values)

| Defense | Hang: mean time to first token | Hang: requests over 0.5 s | 503: mean | 503: calls to the primary |
|---|---|---|---|---|
| neither | 5.06 s | 60 of 60 | 0.82 s | 180 |
| first-token timeout | 1.06 s | 60 of 60 | 0.81 s | 180 |
| circuit breaker | 1.97 s | 23 of 60 | 0.08 s | 5 |
| both | 0.21 s | 9 of 60 | 0.07 s | 5 |

**M1: latency added over calling the mock directly, 10 rounds** (median of the per-round percentiles; 2,000 requests per cell, each caller sends its next request when the last one returns; the mock answers at once; one gateway process)

| Callers | Bare proxy: p50 / p99 | Every check on: p50 / p99 | Requests per second, every check on |
|---|---|---|---|
| 1 | +1.115 / +1.341 ms | +2.446 / +2.994 ms | 307.3 |
| 16 | +20.126 / +116.63 ms | +20.912 / +137.827 ms | 374.75 |
| 64 | +120.303 / +1420.354 ms | +146.831 / +1313.302 ms | 233.25 |

Streamed, time to first token, 1 caller: +1.344 / +1.776 ms as a bare proxy, +2.263 / +2.958 ms with every check on. With a mock that takes 1,000 ms to its first token and 64 callers (640 requests per cell), every check on: -0.287 ms at p50 and +419.863 ms at p99 for streams; -11.308 ms and +399.077 ms non-streamed. At 16 and 64 callers against an instant mock the process is saturated, so those rows measure a queue, not the cost of a request.

**M1b: 64 streamed requests in the same instant, 10 runs of 6 waves** (the mock takes 1,000 ms to its first token; added = gateway minus direct for the same wave; median, min-max)

| Wave | Added, median caller | Added, slowest caller |
|---|---|---|
| 1, after 6 s idle (empty connection pool) | 103.15 ms (86.5-147.8) | 117.25 ms (96.5-199.5) |
| 2 to 6 (64 idle connections in the pool) | 374.65 ms (321.8-502.9) | 498.15 ms (412.9-617.6) |

**M1c: the same burst with httpx alone, no gateway** (64 requests at once to an instant mock, start to last response; httpcore 1.0.9)

| Connection pool | Milliseconds for the burst | Bursts |
|---|---|---|
| empty (64 new connections) | 78.4 (76.6-89.8) | 10 |
| 64 idle keep-alive connections | 370.2 (353.1-406.8) | 50 |

In one profiled burst on the warm pool, httpcore ran its pool bookkeeping (`_assign_requests_to_connections`) 383 times and checked a socket for readability 23,209 times: it walks every pooled connection whenever a request joins or leaves the pool. The gap between these two rows is about the gap between the two rows of M1b, which points at the HTTP client's pool bookkeeping, not the gateway's own checks, as the likely cost of a burst on a full pool. Its share of the time was not isolated, and this is Windows (the readability check is a `select()` call there, `poll()` on Linux), with httpx 0.28.1: not measured on Linux.

Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2);
Python 3.12.4, FastAPI 0.142.2, Starlette 1.7.0, httpx 0.28.1, uvicorn 0.35.0, redis-py 8.1.0, openai 2.4.0,
Redis 8.10.2. Everything runs against the mock provider, so these are the gateway's own costs: a real
provider adds its own latency and failure modes on top. Your numbers will differ; the counts (27, 9, 3 and
exactly 10) should not.
