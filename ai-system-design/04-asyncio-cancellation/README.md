# 04 · The ChatGPT Redis bug: asyncio cancellation and connection pools

Code, tests and measurements for **The ChatGPT Redis Bug: How One Cancelled Request Leaked
Another User's Data**, part 4 of the "AI System Design Case Studies" series on anuptechtips.com:
https://anuptechtips.com/chatgpt-redis-bug-asyncio-cancellation/

On 20 March 2023 a cancelled asyncio request left its reply on a pooled Redis connection, and
the next ChatGPT request on that connection read it as its own. This folder rebuilds that bug
class and the designs that stop it:

- a toy connection pool on asyncio streams, with the buggy version (release in `finally:`) and
  the fixed one (`except BaseException:` close, then re-raise);
- the two other shapes of the same mistake: a slot that never comes back, and a swallowed cancel;
- an owner check for cached values, so a leak becomes a counted cache miss;
- a load test that counts reads returned to the wrong user, for the toy pool, the installed
  redis-py and, optionally, the vulnerable redis-py 4.5.1.

The toy pool and the toy server are small stand-ins written for the article, not production
code. Use them to see the bug, and use a maintained client in a real service.

## Files

| File | What it is |
|---|---|
| `toy_pool.py` | The pool, `query_buggy` and `query` (the post's first code block) |
| `toy_server.py` | A line-based cache server for the toy pool; it can hold its replies for tests |
| `stuck_slot.py` | `query_leaky`: `except Exception` instead of `BaseException`, so a cancel leaks the slot |
| `swallowed_cancel.py` | `getconn_swallowing` and `getconn`: what catching a cancel does to the caller's timeout |
| `owner_check.py` | `set_profile`, `owned_by`, `get_profile` with the mismatch counter (the second code block) |
| `delay_proxy.py` | A TCP proxy that delays every chunk in both directions (idea: redis-py issue #2665) |
| `precise_loop.py` | Millisecond timers for the measurements on Windows (a no-op elsewhere) |
| `load_test.py` | One load run: 10,000 reads, 200 tasks, a 10-connection pool, random 5-120 ms timeouts |
| `repro_cancel_then_get.py` | Cancel a `GET` mid-flight, then `GET` another key (after issue #2665) |
| `handshake.py` | What the installed redis-py sends when it opens a connection, before your command (`--lean`: with `protocol=2, driver_info=None`) |
| `measure_load.py` | M1, M2, M4: the load test, 20 runs per client |
| `measure_fix_cost.py` | M3: a `GET` on a warm connection vs one that must reconnect; the owner check's cost |
| `measure_repro.py` | M2, M4: the cancel-then-GET script, 20 runs per redis-py version, without and with a pause |
| `tests/` | pytest: the toy pool, the other two shapes, the owner check, redis-py behind the proxy |
| `results/` | Raw CSV/JSON from every run |

## Run it

You need Python 3.12+ and Redis on `localhost:56379`. The repo's Docker setup provides it: run
`docker compose up -d` in `reliable-python-services/` (see `docker-compose.yml` there). Everything
here uses Redis DB 9 and nothing else; the tests flush that DB, so don't run them while a
measurement is going.

```shell
pip install "redis>=8" pytest
cd ai-system-design/04-asyncio-cancellation
pytest -q
python measure_load.py          # about 55 minutes: 20 runs of 8 clients, 20 s each
python measure_fix_cost.py
python measure_repro.py
python handshake.py --lean --save
```

`python measure_load.py --runs 2` is the quick look. `--resume` keeps the runs already in
`results/load_runs.csv` and does only the missing ones. The toy-pool runs don't need a Redis
server: `python load_test.py --client toy-buggy` and `python load_test.py --client toy-fixed`
print one JSON line each.

### Optional: reproduce the bug with redis-py 4.5.1

redis-py 4.5.1 is the version in the first 2023 bug report (#2624). It is **known to be vulnerable**
(CVE-2023-28858, CVE-2023-28859). Install it only in a throwaway virtual environment, point it
only at a local test Redis, and never use it in a real service.

```shell
python -m venv .venv-redis-4.5.1
.venv-redis-4.5.1/bin/pip install redis==4.5.1        # Windows: .venv-redis-4.5.1\Scripts\pip
export REDIS_451_PYTHON=$PWD/.venv-redis-4.5.1/bin/python
pytest -q                                              # 5 more tests run instead of skipping
python measure_load.py --old-python $REDIS_451_PYTHON
python measure_repro.py --old-python $REDIS_451_PYTHON
python measure_fix_cost.py --old-python $REDIS_451_PYTHON
python repro_cancel_then_get.py                        # with the installed redis-py: bar: b'bar'
$REDIS_451_PYTHON repro_cancel_then_get.py             # with 4.5.1: bar: b'foo'
```

The environment is not part of this repo (`.venv*/` is ignored). Delete it when you are done.

## Results

AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM, Windows 11, Docker Desktop 28.5 (WSL2);
Python 3.12.4, Redis 8.10.2 in Docker, redis-py 8.1.0 and, from a throwaway environment,
redis-py 4.5.1. Your counts and timings will differ; the zeros shouldn't.

### The load test (`results/load_runs.csv`, `results/load_summary.json`)

10,000 reads of `user:{id}` (1,000 users) from 200 tasks through a 10-connection pool. A delay
proxy holds every chunk for 5 ms each way; each read has its own timeout, drawn between 5 and
120 ms from a seeded generator; each task pauses up to 600 ms between reads. 20 runs per client,
one seed per run (1 to 20). Each run is a fresh process, and the clients take turns, in reverse
order every other round. The `--lean-handshake` client was added after the first rounds, so its
runs 1 to 6 ran back to back; runs 7 to 20 took turns with the others. The lab is one Redis node
and redis-py's standalone client (`BlockingConnectionPool`), not Redis Cluster. Each cell is the
median, with the lowest and highest run in brackets when they differ. p99 is over the reads that
returned in time.

| Client | Wrong-owner reads | Caught by the owner check | Errors | Timed out | Connections opened | p99 (ms) |
|---|---|---|---|---|---|---|
| Toy pool, release in `finally:` | 9,820.5 (9,741 to 9,893) | 0 | 0 | 32 (26 to 36) | 10 (8 to 10) | 11.8 (10.8 to 12.3) |
| redis-py 4.5.1 | 2,143.5 (1,869 to 2,524) | 0 | 30 (21 to 39) | 597.5 (533 to 661) | 575.5 (519 to 631) | 26.8 (26.3 to 27.5) |
| Toy pool, close on cancel | 0 | 0 | 0 | 668 (579 to 736) | 660 (577 to 726) | 20.8 (19.1 to 22.4) |
| redis-py 8.1.0 | 0 | 0 | 0 | 1,921.5 (1,763 to 2,357) | 1,415 (1,331 to 1,606) | 74.5 (70.2 to 80.2) |
| Toy pool (release) + owner check | 0 | 9,813.5 (9,769 to 9,889) | 0 | 31 (24 to 35) | 10 (8 to 10) | 11.8 (10.9 to 12.3) |
| redis-py 4.5.1 + owner check | 0 | 2,206 (1,990 to 2,469) | 0 | 598.5 (569 to 659) | 572.5 (542 to 628) | 27.0 (26.5 to 27.9) |
| redis-py 8.1.0 + owner check | 0 | 0 | 0 | 1,993 (1,614 to 2,280) | 1,450 (1,266 to 1,582) | 74.0 (68.7 to 79.9) |
| redis-py 8.1.0, `--lean-handshake` | 0 | 0 | 0 | 820.5 (737 to 911) | 799 (725 to 881) | 30.8 (29.7 to 32.9) |
| redis-py 8.1.0, no timeouts | 0 | 0 | 0 | 0 | 10 | 22.8 (20.5 to 28.0) |
| Toy pool (close), no timeouts | 0 | 0 | 0 | 0 | 10 | 22.1 (19.6 to 25.2) |

- The toy pool that releases in `finally:` never closes a connection, so a connection that is one
  reply behind stays behind for the rest of the run.
- redis-py 4.5.1 also releases on a cancel, but its pool looks for unread data when it hands a
  connection out and reconnects if it finds any. Only a stale reply that is still on the wire
  gets through. Its errors were all `JSONDecodeError`; the replies that failed to parse:
  `OK` 604 times over the 20 runs. That is the answer to the `SELECT 9` of a reconnect whose
  handshake was cancelled.
- redis-py 8.1.0 closes the connection on a cancel and never returned a wrong profile. The next
  read on that slot pays the connection handshake, which is why it timed out more often than
  4.5.1 did. `--lean-handshake` (`protocol=2, driver_info=None`: one round trip instead of
  three) shows how much of that is the handshake.

### Cancel, then GET (`results/repro_runs.csv`, `results/repro_summary.json`)

`repro_cancel_then_get.py`: the proxy delays each chunk by 100 ms, `GET repro:foo` is cancelled
50 ms in, then the script asks for `repro:bar`.

| redis-py | Runs | Second GET returned the first GET's value | Second GET returned its own value | Connections opened |
|---|---|---|---|---|
| redis-py 4.5.1 | 20 | 20 | 0 | 1 |
| redis-py 4.5.1, 300 ms pause before the second GET | 20 | 0 | 20 | 2 |
| redis-py 8.1.0 | 20 | 0 | 20 | 2 |
| redis-py 8.1.0, 300 ms pause before the second GET | 20 | 0 | 20 | 2 |

With a 300 ms pause the stale reply has arrived before the second `GET`, and 4.5.1's pool finds
it and reconnects. The output of one run per version is in `results/repro_output_redis-py-*.txt`.

### What the fix costs (`results/fix_cost.json`, `results/fix_cost_samples.csv`)

A `GET` through the same proxy (5 ms each way), 400 samples per row, in milliseconds:

| GET on | Median | Min | Max |
|---|---|---|---|
| redis-py, warm connection | 12.02 | 10.73 | 14.24 |
| redis-py, closed connection | 49.81 | 46.54 | 53.84 |
| toy pool, warm connection | 11.63 | 10.36 | 13.86 |
| toy pool, closed connection | 12.48 | 11.21 | 15.57 |

What a new connection sends before the command (`handshake.py`):

- redis-py 8.1.0: 3 round trips before the command: `HELLO 3`; then `CLIENT MAINT_NOTIFICATIONS`; then `CLIENT SETINFO` + `CLIENT SETINFO` + `SELECT 9`
- redis-py 4.5.1: 1 round trip before the command: `SELECT 9`
- redis-py 8.1.0 with `protocol=2, driver_info=None` (`handshake.py --lean`, `results/handshake_lean.json`): 1 round trip before the command: `SELECT 9`

The owner check: `owned_by()` took 2,017 ns per call against 1,934 ns for
`json.loads()` alone on a 64-byte value (medians of 20 rounds of
200,000 calls). The difference per round had a median of 88 ns
and ranged from -68 to 376 ns, so the check's
cost is inside the noise of this measurement.
