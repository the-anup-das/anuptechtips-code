"""One load run: 10,000 cache reads from 200 tasks through a 10-connection pool that sits
behind a delay proxy, each read under its own random timeout. Prints one JSON line.

    python load_test.py --client toy-buggy
    python load_test.py --client toy-fixed
    python load_test.py --client redis                 # the redis-py this Python has
    python load_test.py --client redis --owner-check
    python load_test.py --client redis --no-timeouts   # baseline: nothing gets cancelled
    python load_test.py --client redis --lean-handshake  # connect with SELECT only (redis-py 8)

A read counts as "wrong owner" when it returns, without an error and inside its timeout,
a profile whose user_id is not the one that was asked for.
"""
import argparse
import asyncio
import json
import math
import platform
import random
import time
from collections import Counter

import redis as redis_sync
import redis.asyncio as redis

from delay_proxy import DelayProxy
from owner_check import get_profile, metrics
from precise_loop import run
from toy_pool import Pool, query, query_buggy
from toy_server import ToyServer, profile_for

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 56379, 9


class ToyClient:
    """Gives the toy pool the one method this test calls on redis-py: get(key)."""

    def __init__(self, pool: Pool, query_fn) -> None:
        self.pool, self.query = pool, query_fn

    async def get(self, key: str) -> str:
        return await self.query(self.pool, key)


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100 * len(ordered)) - 1)]


async def worker(client, rng: random.Random, count: int, args, stats: dict) -> None:
    for _ in range(count):
        await asyncio.sleep(rng.uniform(0, args.think_ms) / 1000)
        user_id = rng.randrange(args.users)
        timeout = None if args.no_timeouts else rng.uniform(*args.timeout_ms) / 1000
        started = time.perf_counter()
        try:
            async with asyncio.timeout(timeout):
                if args.owner_check:
                    profile = await get_profile(client, user_id)
                    got = None if profile is None else profile["user_id"]
                else:
                    got = json.loads(await client.get(f"user:{user_id}"))["user_id"]
        except TimeoutError:
            stats["timed_out"] += 1
            continue
        except Exception as exc:  # what a handler would turn into a 500
            stats["errors"][type(exc).__name__] += 1
            if isinstance(exc, json.JSONDecodeError):  # keep what the reply was instead
                stats["undecodable"][exc.doc[:20]] += 1
            continue
        stats["latencies"].append(time.perf_counter() - started)
        if got is None:
            stats["misses"] += 1
        elif got == user_id:
            stats["correct"] += 1
        else:
            stats["wrong_owner"] += 1


async def main(args) -> dict:
    keys = {f"user:{i}": json.dumps(profile_for(i)) for i in range(args.users)}
    if args.client == "redis":
        library = f"redis-py {redis_sync.__version__}"
        direct = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        direct.mset(keys)
        proxy = DelayProxy(REDIS_HOST, REDIS_PORT, args.delay_ms / 1000)
        # RESP2 and no CLIENT SETINFO: opening a connection is then one round trip, not three
        lean = {"protocol": 2, "driver_info": None} if args.lean_handshake else {}
        pool = redis.BlockingConnectionPool(host="127.0.0.1", port=await proxy.start(), db=REDIS_DB,
                                            max_connections=args.pool, timeout=None, **lean)
        client = redis.Redis(connection_pool=pool)
    else:
        library = "toy pool"
        server = ToyServer()
        proxy = DelayProxy("127.0.0.1", await server.start(), args.delay_ms / 1000)
        pool = Pool("127.0.0.1", await proxy.start(), size=args.pool)
        client = ToyClient(pool, query_buggy if args.client == "toy-buggy" else query)

    metrics.clear()
    stats = {"correct": 0, "wrong_owner": 0, "misses": 0, "timed_out": 0,
             "errors": Counter(), "undecodable": Counter(), "latencies": []}
    per_task = args.requests // args.tasks
    started = time.perf_counter()
    await asyncio.gather(*(
        worker(client, random.Random(args.seed * 100_000 + task), per_task, args, stats)
        for task in range(args.tasks)))
    seconds = time.perf_counter() - started

    if args.client == "redis":
        await pool.disconnect()
        direct.delete(*keys)
        direct.close()
    else:
        pool.close()
        server.close()
    proxy.close()

    latencies = stats.pop("latencies")
    errors = stats.pop("errors")
    undecodable = stats.pop("undecodable")
    return {
        "client": args.client, "owner_check": args.owner_check, "library": library,
        "python": platform.python_version(), "seed": args.seed, "requests": per_task * args.tasks,
        "tasks": args.tasks, "pool": args.pool, "delay_ms": args.delay_ms,
        "timeout_ms": None if args.no_timeouts else args.timeout_ms, "think_ms": args.think_ms,
        **stats,
        "owner_mismatches_caught": metrics["cache_owner_mismatch"],
        "errors": sum(errors.values()), "error_types": dict(errors),
        "undecodable_replies": dict(undecodable),
        "connections_opened": proxy.connections,
        "p50_ms": round(percentile(latencies, 50) * 1000, 2) if latencies else None,
        "p99_ms": round(percentile(latencies, 99) * 1000, 2) if latencies else None,
        "seconds": round(seconds, 2),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", choices=["toy-buggy", "toy-fixed", "redis"], required=True)
    parser.add_argument("--owner-check", action="store_true")
    parser.add_argument("--no-timeouts", action="store_true")
    parser.add_argument("--lean-handshake", action="store_true")
    parser.add_argument("--requests", type=int, default=10_000)
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--pool", type=int, default=10)
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--delay-ms", type=float, default=5, help="proxy delay, each way")
    parser.add_argument("--timeout-ms", type=float, nargs=2, default=[5, 120])
    parser.add_argument("--think-ms", type=float, default=600, help="max pause between reads")
    parser.add_argument("--seed", type=int, default=1)
    print(json.dumps(run(main(parser.parse_args()))))
