# System design patterns

Part of [anuptechtips-code](../README.md), the code behind the articles on anuptechtips.com.

Code and tests for **System Design Patterns: The 6 Every Engineer Must Learn**, the overview post
for the System Design series on anuptechtips.com:
https://anuptechtips.com/system-design-patterns/

The post walks through six patterns (scale reads, scale writes, real-time data, long-running
jobs, reliability, CQRS), each with a real case. The code here is the cache from pattern 1,
built to avoid both mistakes behind Facebook's 23 September 2010 outage:

- when an entry expires, one caller refills it and the others wait for that result, so a
  thousand misses on one key cost one database query, not a thousand;
- when the refill fails, the old value is kept for a few more seconds (`grace`) instead of
  being treated as a new value, so a struggling database sees less traffic, not more;
- with nothing old to fall back on, the error is raised.

The deep dives for the other patterns are in [reliable-python-services](../reliable-python-services/)
(idempotency keys, rate limiting, the transactional outbox, idempotent consumers, Redis locks)
and [ai-system-design](../ai-system-design/) (fail open vs fail closed).

Until the post is published, its link returns 404.

## Files

| File | What it is |
|---|---|
| `cache_aside.py` | `Cache`: cache-aside with one refill per key and the stale-on-error grace period. The code block in the post |
| `tests/test_cache_aside.py` | 200 threads on one expired key make one database call; a failed refill serves the old value and backs off; a failed first load raises |

## Run it

You need Python 3.12+ and pytest. No Docker, no services: the cache keeps entries in memory
to stay short. Run these from this folder:

```sh
pip install pytest
pytest -q
```

With Redis instead of a dict, the per-key `threading.Lock` becomes a `SET NX` lock shared by every
process. [reliable-python-services/05-redis-locks](../reliable-python-services/05-redis-locks/)
covers that lock and where it breaks.

Found a case that breaks this? Open an issue or a pull request.
