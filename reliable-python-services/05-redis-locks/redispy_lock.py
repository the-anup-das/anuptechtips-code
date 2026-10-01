"""redis-py's built-in Lock, with the two defaults that bite changed.

redis-py 8.1.0: timeout=None (the key never expires) and blocking_timeout=None
(acquire waits forever). Set both, every time.
"""
from collections.abc import Awaitable, Callable

import redis
import redis.asyncio as aredis
from redis.exceptions import LockError, LockNotOwnedError


def run_exclusive(r: redis.Redis, name: str, job: Callable[[], None],
                  ttl: float = 30, wait: float = 5) -> bool:
    """Run job() under the lock. False if another worker held it for `wait` seconds."""
    try:
        with r.lock(name, timeout=ttl, blocking_timeout=wait):
            job()
    except LockNotOwnedError:
        raise  # the lease ran out mid-job, so another worker may have run it too
    except LockError:
        return False  # never got the lock (LockNotOwnedError is a subclass: catch it first)
    return True


async def run_exclusive_async(r: aredis.Redis, name: str, job: Callable[[], Awaitable[None]],
                              ttl: float = 30, wait: float = 5) -> bool:
    """The same with redis.asyncio: identical arguments, awaited."""
    try:
        async with r.lock(name, timeout=ttl, blocking_timeout=wait):
            await job()
    except LockNotOwnedError:
        raise
    except LockError:
        return False
    return True
