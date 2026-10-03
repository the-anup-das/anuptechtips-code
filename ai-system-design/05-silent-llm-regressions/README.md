# 05 · Silent LLM regressions: sliced evals, release gates and flag-based rollback

Code, tests and measurements for **Silent LLM Regressions: Anthropic and OpenAI Postmortems**,
part 5 of the "AI System Design Case Studies" series on anuptechtips.com:
https://anuptechtips.com/silent-llm-regressions/

The post reads four published incidents (Anthropic, September 2025 and April 2026; OpenAI,
February 2024 and April 2025) and asks why the evals stayed green. This folder rebuilds the
failure on a deterministic stand-in model, with no real LLM and no paid API, and tests the design
that catches it:

- probes with known answers go down the production path (sticky router, server pool, batching,
  sampler), and every reply becomes an event tagged with the slice it landed in;
- a Kafka consumer keeps one counter row per slice in Postgres, and counts every event once;
- a Wilson-bound check alerts on the worst slice, where a global pass rate barely moves;
- a release gate blocks on behavior evals and A/B guardrails, and never reads the thumbs-up rate;
- model and prompt versions live in one Redis hash, so a rollback is one `HSET`.

The bugs are injected on purpose. The numbers show what each detector catches; they say nothing
about how any real model behaves.

## Files

| File | What it is |
|---|---|
| `fake_model.py` | the stand-in: hashed logits, then top-k, softmax, top-p, greedy at temperature 0; the injected bugs (`approx_top_k`, the fp16 pick, `noise`, `script_boost`), the `agree_bias` knob and the word cap read from the system prompt |
| `probes.py` | 200 known-answer, 100 wrong-premise and 50 multi-step probes, and how a reply is scored |
| `serving.py` | the production path: sticky router, pools, hardware, batches; one event per probe |
| `detectors.py` | flags replies with letters from a script the prompt didn't use |
| `schema.sql` | `slice_stats`, `processed_events` and `eval_runs` |
| `pipeline.py` | events to Kafka, and the consumer that records them without double counting |
| `slices.py` | `wilson_bounds` and `degraded_slices`: the per-slice (and global) alert |
| `gate.py` | `gate()` and `worse()`: the release gate |
| `rollout.py` | the Redis flag: sticky cohorts, `widen()` with a soak timer, `rollback()` |
| `flag_cache.py` | the same flag cached in-process for a TTL (used by M6 only) |
| `evals.py` | the offline suite, and saving and loading its results in Postgres |
| `ablation.py` | the suite with each system-prompt line removed |
| `users.py` | simulated users who thumb up agreement more than correctness; a 2% A/B test |
| `harness.py` | per-run schemas, topics and consumer threads for the tests and measurements |
| `measure_*.py` | M1 to M6, below |
| `tests/` | pytest against the Docker services, one test per path the post describes |
| `results/` | raw CSV/JSON from every run |

## Run it

The services are the ones from `reliable-python-services/docker-compose.yml` (PostgreSQL 18,
Redis 8, Kafka 4.3): `docker compose up -d` in that folder. This post uses Postgres database
`regress` (create it once: `CREATE DATABASE regress;`), Redis DB 10 and Kafka topics that start
with `regress.`. Override with `REGRESS_DSN`, `REGRESS_REDIS_URL` and `KAFKA_BROKERS`.

```shell
pip install "psycopg[binary]>=3.2" "redis>=8" confluent-kafka numpy pytest matplotlib
cd ai-system-design/05-silent-llm-regressions
pytest -q
python measure_misrouting.py
python measure_topk.py
python measure_script.py
python measure_ablation.py
python measure_sycophancy.py
python measure_rollback.py
```

`python pipeline.py` runs the consumer on its own, against the topic `regress.events`.

## Results

Every measurement ran 20 times; numbers are medians, with the range where it matters.

**M1 · sticky misrouting** (`measure_misrouting.py`). 60 ticks of 2,000 known-answer probes
through `serve()`, Kafka and the consumer. The router misroutes 0.8% of new sessions from tick
10, and ramps to 16% between ticks 40 and 49. Alerts look at the last 3 ticks.

| | Median | Range |
|---|---|---|
| Events counted, of 120,000 sent per run | 120,000 | 120,000-120,000 |
| Global pass rate, healthy ticks 0-9 | 97.37% | 97.10-97.58% |
| Global pass rate at 0.8% misrouted | 97.18% | 96.98-97.28% |
| Global pass rate at 16% misrouted | 93.99% | 93.76-94.16% |
| Misrouted slice's pass rate at 0.8% | 76.07% | 71.43-80.13% |
| Sliced alert (by pool) first fires at tick | 12 | 11-14 |
| Global alert first fires at tick | 45 | 43-47 |
| Global alert minus sliced alert, in ticks | 34 | 29-36 |
| False alarms, any dimension, any tick | 0 | 0-0 |
| Users misrouted at least once by tick 40 | 21.7% | 17.4-25.0% |

**M2 · the top-k bug and the fp16 pick** (`measure_topk.py`). 9,600 known-answer requests per
cell, compared with the reference (exact top-k, fp32, batch size 1).

| Sampler | Batch 1 | Batch 8 | Batch 32 | Batch 64 |
|---|---|---|---|---|
| exact top-k, fp32 pick | 97.27% | 97.27% | 97.27% | 97.27% |
| exact top-k, fp16 pick | 97.27% | 97.27% | 97.27% | 97.27% |
| approximate top-k (injected bug) | 97.27% | 97.27% | 93.46% | 93.46% |

The fp16 pick changed at least one token in 105 replies (88-128) and the answer in 0 (0-2), at
every batch size. The buggy kernel changed nothing at batch 1 and 8; at 32 and 64 it changed the
answer in 385 replies (375-393) and at least one token in 3,933 (3,838-4,020).

**M3 · the script detector** (`measure_script.py`). 10,000 replies per run.

| | Boost off | Boost on |
|---|---|---|
| Replies flagged | 198.5 (195-200), all translation probes | 640 (597-679) |
| Flagged, not a translation probe | 0 | 443 (401-480) |
| Known answers right | 9,726.5 (9,700-9,761) | 9,698.5 (9,670-9,729) |
| Flagged but still right (not translation) | 0 | 402.5 (360-436) |

**M4 · prompt ablation** (`measure_ablation.py`). 7,000 requests per run of the suite; pass rate
over all 350 probes, and over the 50 multi-step ones.

| Prompt | model-1, all | model-1, multi-step | model-2, all | model-2, multi-step |
|---|---|---|---|---|
| all six lines | 94.20% | 86.25% | 95.24% | 87.35% |
| without the 25-word line | 95.73% | 97.50% | 96.81% | 98.30% |
| without any other line | 94.11-94.22% | 85.90-86.15% | 95.09-95.34% | 87.10-87.30% |

`harmful_lines()` named the 25-word line (on `multi_step`) in 20 of 20 runs for both models, and
no other line in any run.

**M5 · the sycophancy trap** (`measure_sycophancy.py`).

| `agree_bias` | Thumbs-up | Known answers right | Wrong premises corrected | Accuracy-only gate | `gate()` |
|---|---|---|---|---|---|
| 0 | 40.1% | 97.4% | 91.6% | ships 20/20 | ships 20/20 |
| 1 | 41.5% | 97.4% | 78.1% | ships 20/20 | blocks 20/20 |
| 2 | 43.8% | 97.2% | 52.4% | ships 20/20 | blocks 20/20 |
| 3 | 46.3% | 97.3% | 26.4% | ships 20/20 | blocks 20/20 |

The thumbs picked `agree_bias` 3 in 20 of 20 runs. In the 2% A/B test of that pick: thumbs-up
46.5% against 40.2% for production; wrong premises corrected 27.0% against 91.8%. `gate()`
blocked it in 20 of 20 runs, on the `wrong_premise` suite and on the A/B guardrail.

**M6 · rollback** (`measure_rollback.py`, inside the shared measure lock). 8 workers on the
candidate; milliseconds from calling `rollback()`.

| | Read per request | Flag cached for 1 s |
|---|---|---|
| `rollback()` call | 0.85 ms (0.48-2.53) | 0.65 ms (0.49-0.77) |
| First worker on the stable version | 0.99 ms (0.74-2.33) | 113.4 ms (90.3-116.2) |
| Last worker on the stable version | 2.12 ms (1.8-3.19) | 988.5 ms (965.8-990.8) |
| Candidate replies after `rollback()` returned | 0 (0-0) | 2,967.5 (2,829-3,130) |

A flag flip on a toy says nothing about rolling back a real model fleet.

Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5
(WSL2); Python 3.12, NumPy 1.26.4, psycopg 3.3.6, redis-py 8.1.0, confluent-kafka 2.15.1,
PostgreSQL 18.6, Redis 8.10.2, Kafka 4.3.1. The stand-in is deterministic, so the counts should
come out the same on your machine; the M6 timings won't.
