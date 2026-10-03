# 01 · A support bot that can't ship invented policy (Python, PostgreSQL, Redis, Kafka)

Code, tests and measurements for **Air Canada Chatbot Hallucination: Why Support Bots Invent
Policy, and the Design That Stops It**, part 1 of the "AI System Design Case Studies" series on
anuptechtips.com: https://anuptechtips.com/air-canada-chatbot-hallucination/

It shows a support bot whose answers have to clear four gates before a customer sees them:

- a versioned policy store in Postgres (`policy_clause`, one row per version, `WITHOUT OVERLAPS`);
- gate 1, scope: a keyword intent router and a retrieval that abstains below a threshold;
- gate 2, the commitment guard: promises (refunds, prices, "binding") are answered by
  `eligibility()` from the booking record, or handed off;
- gate 3, tier 1: citation in force, policy type = intent, numbers / durations / negations,
  wording;
- gate 4, tier 2: a judge, only for drafts tier 1 flags (a rule-based stand-in here);
- an AI label on every reply, a Redis kill switch, a cache key that carries the clause versions,
  and an audit row plus an outbox event in one transaction, relayed to Kafka `chatbot.audit`.

There is no model and no API key. `fake_llm.py` is a seeded stand-in that fails on purpose
(blends two clauses, quotes the wrong clause, invents a rule, agrees with the customer), so the
gates have something to catch. The airline, Juniper Air, doesn't exist.

## Files

| File | What it is |
|---|---|
| `schema.sql` | `policy_clause`, `answer_audit`, `outbox` |
| `corpus.py` | the 40 policy clauses (8 types of 5) |
| `questions.py` | the 300 test questions and their answer key |
| `db.py` | connection settings, `reset()`, `publish_clause()` |
| `bot.py` | `retrieve()`, `route()`, `answer_naive()` and `answer()` (the gates) |
| `checks.py` | `tier1_check()`, the `judge()` stand-in, the `COMMITMENT` pattern |
| `eligibility.py` | the backend's refund rules |
| `fake_llm.py` | the deterministic stand-in model |
| `service.py` | `handle()`: kill switch, versioned cache, audit row + outbox event |
| `relay.py` | outbox relay to Kafka (at-least-once) |
| `rebuild.py` | rebuild the audit trail from Kafka and compare it with Postgres |
| `measure_*.py` | the measurements below |
| `tests/` | pytest against the Docker services |
| `results/` | raw CSV/JSON from every run |

## Run it

Start the services from `reliable-python-services/` in this repo (`docker compose up -d`:
PostgreSQL 18 on 55432, Redis 8 on 56379, Kafka 4 on 59092). The code uses Postgres database
`chatbot` (created on first run), Redis DB 6 and Kafka topics starting with `chatbot.`.
Override with `CHATBOT_DSN`, `CHATBOT_REDIS_URL` and `CHATBOT_KAFKA`.

```shell
cd ai-system-design/01-chatbot-hallucination
python db.py
pytest -q
python measure_gates.py
python measure_tau.py
python measure_cache.py
python measure_audit.py
```

`python measure_latency.py` and `python measure_retrieval.py` produce the two smaller results.

## Results

All counts are out of 300 questions, medians of 20 seeds (min to max). A shipped answer is
*unsafe* unless it is the right clause, unchanged in meaning, or a backend decision. A *false
hand-off* is a hand-off of a question the stand-in had answered correctly.

**M1: four configurations** (`measure_gates.py`)

| Configuration | Unsafe shipped | False hand-offs | Hand-offs | Drafts to tier 2 |
|---|---|---|---|---|
| no gates | 178.5 (171–190) | 0 | 0 | – |
| + scope and threshold (τ = 0.2) | 117.5 (110–130) | 5 (2–7) | 67 | – |
| + answer check (tiers 1 and 2) | 11 (7–19) | 24 (18–30) | 190.5 (185–203) | 175 of 233 |
| + commitment guard | 10 (5–16) | 24 (18–30) | 190 (184–202) | 167.5 of 233 |

Ablation, answer check without the policy-type check: 29.5 unsafe (23–42), 6 false hand-offs;
all 21 wrong-clause quotes that reached the check shipped. With the type check, 13 were stopped
and 8 shipped; 18 of the 24 false hand-offs are the type check's (the keyword router is right
for 162 of the 200 questions that have a clause).

The commitment guard's effect (11 to 10) is about 2 answers per run and comes from three leading
questions. In the first run the guard caught nothing the answer check hadn't, so four leading
questions were rewritten to reuse a clause's own wording. Of the 60 backend answers over the 20
seeds, 31 would have been handed off by the answer check anyway. The tier-2 judge is a rule-based
stand-in (same fact checks, wording threshold 0.6 instead of 0.75, small talk skipped), not a model.

What still shipped with all gates (medians by type): 8 wrong-clause quotes of the right policy type, 1 retrieval
miss (a guide-dog question in 15 of 20 seeds, a refund demand for ticket Z6H2RY in 12 of 20), 1
agreement ("a full year", 14 of 20 seeds: tier 1 flagged the wording, the stand-in judge passed it,
and the fact check reads digits only).

**M2: the abstain threshold** (`measure_tau.py`)

| τ | scope only: unsafe / false hand-offs | all gates: unsafe / false hand-offs | drafts |
|---|---|---|---|
| 0 | 121.5 / 1 | 10 / 21 | 242 |
| 0.1 | 121.5 / 1 | 10 / 21 | 242 |
| 0.2 | 117.5 / 5 | 10 / 24 | 233 |
| 0.3 | 92.5 / 19 | 8 / 34 | 193 |
| 0.4 | 64.5 / 32 | 7 / 43.5 | 154 |
| 0.5 | 56.5 / 45 | 7 / 55.5 | 132 |
| 0.6 | 27.5 / 64 | 5 / 71.5 | 84 |

**M3: latency, one call at a time** (`measure_latency.py`, inside the repo's measure lock)

| Step | Calls | p50 | p95 |
|---|---|---|---|
| `route()` | 6,000 | 27 µs | 47 µs |
| `retrieve()` | 6,000 | 900 µs | 1772 µs |
| `tier1_check()` | 5,740 | 116 µs | 261 µs |
| `judge()` | 5,740 | 39 µs | 156 µs |

**M4: cache** (`measure_cache.py`): 1,000 requests, clause REF-03 republished at request 500,
20 runs. Question-only key: 13 stale answers (6–19), in all 20 runs. Versioned key: 0.

**M5: audit trail** (`measure_audit.py`): 300 replies, relay killed after Kafka acked its 6th
batch of 25 and before marking it, 20 runs: 325 events in the topic, 25 duplicates, 300 records
rebuilt, 0 mismatches with `answer_audit`, in every run.

**Retrieval** (`measure_retrieval.py`): ordering by word coverage, then `ts_rank_cd`, put the
right clause first for 138 of 200 questions and in the top 5 for 190; `ts_rank_cd` alone, 119
and 188.

Setup: AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5
(WSL2); Python 3.12, psycopg 3.3.6, redis-py 8.1.0, confluent-kafka 2.15.1, PostgreSQL 18.6,
Redis 8.10.2, Kafka 4.3.1. The counts depend on the stand-in's failure rates (`fake_llm.RATES`)
and on the question set; change them and rerun.
