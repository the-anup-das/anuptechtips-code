"""Renew a redis-py lock in the background, and tell the work when renewal fails.

Renewal keeps a *slow* holder's lease alive. It can't help a *frozen* one: a
process that's paused (SIGSTOP, docker pause, VM steal, swap) freezes the
renewer too, and the lease runs out anyway. A blocked event loop does the same
to the asyncio version. So the work must check `lost`, and the data still needs
a fencing token (fencing.py).
"""
import asyncio
import contextlib
import sys
import threading
import time
from collections.abc import AsyncIterator, Iterator

import redis
import redis.asyncio as aredis
from redis.exceptions import LockError


@contextlib.contextmanager
def renewing_lock(r: redis.Redis, name: str, ttl: float, wait: float) -> Iterator[threading.Event]:
    # thread_local=False: the renewer thread must see the token the main thread got
    lock = r.lock(name, timeout=ttl, blocking_timeout=wait, thread_local=False)
    if not lock.acquire():
        raise LockError(f"{name} is busy", lock_name=name)
    lost, stop = threading.Event(), threading.Event()

    def renew() -> None:
        while not stop.wait(ttl / 3):  # renew at a third of the TTL
            try:
                lock.extend(ttl, replace_ttl=True)
            except LockError:  # expired or taken: someone else may be working now
                lost.set()
                return

    renewer = threading.Thread(target=renew, daemon=True)
    renewer.start()
    try:
        yield lost  # the work checks lost.is_set() between steps and stops if it's set
    finally:
        stop.set()
        renewer.join()
        if not lost.is_set():
            lock.release()  # LockNotOwnedError here means it expired after the last renewal


@contextlib.asynccontextmanager
async def renewing_lock_async(r: aredis.Redis, name: str, ttl: float,
                              wait: float) -> AsyncIterator[asyncio.Event]:
    lock = r.lock(name, timeout=ttl, blocking_timeout=wait)
    if not await lock.acquire():
        raise LockError(f"{name} is busy", lock_name=name)
    lost = asyncio.Event()

    async def renew() -> None:
        while True:
            await asyncio.sleep(ttl / 3)  # only runs if the event loop gets a turn
            try:
                await lock.extend(ttl, replace_ttl=True)
            except LockError:
                lost.set()
                return

    renewer = asyncio.create_task(renew())
    try:
        yield lost
    finally:
        renewer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await renewer
        if not lost.is_set():
            await lock.release()


if __name__ == "__main__":
    # Used by tests/test_renewing.py: hold a renewing lock and report when it's lost.
    # python renewing.py <redis_url> <lock name> <ttl seconds> <hold seconds>
    url, name, ttl, hold = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
    with renewing_lock(redis.Redis.from_url(url), name, ttl=ttl, wait=1) as lost:
        print("acquired", flush=True)
        deadline = time.monotonic() + hold
        while time.monotonic() < deadline:
            if lost.is_set():
                print("lost", flush=True)
                sys.exit(3)
            time.sleep(0.05)
    print("done", flush=True)
