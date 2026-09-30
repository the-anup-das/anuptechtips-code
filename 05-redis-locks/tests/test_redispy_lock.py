import asyncio
import threading
import time

import pytest
import redis.asyncio as aredis
from redis.exceptions import LockError, LockNotOwnedError
from redis.lock import Lock

from conftest import REDIS_URL
from redispy_lock import run_exclusive, run_exclusive_async


def test_defaults_in_this_redis_py(r):
    lock = r.lock("t:defaults")
    assert (lock.timeout, lock.blocking_timeout, lock.sleep, lock.blocking) == (None, None, 0.1, True)
    assert lock.thread_local is True
    assert lock.acquire()
    assert r.pttl("t:defaults") == -1  # no TTL: if this process dies, the lock stays forever
    lock.release()


def test_lock_expires_at_timeout(r):
    lock = r.lock("t:job", timeout=0.5)
    assert lock.acquire()
    assert 0 < r.pttl("t:job") <= 500
    time.sleep(0.6)
    assert r.exists("t:job") == 0
    assert r.lock("t:job", timeout=5).acquire(blocking=False)


def test_late_release_raises_and_keeps_the_new_holders_lock(r):
    a = r.lock("t:job", timeout=0.3)
    assert a.acquire()
    time.sleep(0.4)
    b = r.lock("t:job", timeout=5)
    assert b.acquire(blocking=False)
    with pytest.raises(LockNotOwnedError):
        a.release()
    assert b.owned()


def test_context_manager_raises_lockerror_when_busy(r):
    assert r.lock("t:job", timeout=5).acquire()
    with pytest.raises(LockError, match="Unable to acquire lock"):
        with r.lock("t:job", timeout=5, blocking_timeout=0.3):
            pass


def test_run_exclusive_skips_when_busy(r):
    assert r.lock("t:job", timeout=5).acquire()
    ran = []
    assert run_exclusive(r, "t:job", lambda: ran.append(1), ttl=5, wait=0.3) is False
    assert ran == []


def test_run_exclusive_runs_and_releases(r):
    ran = []
    assert run_exclusive(r, "t:job", lambda: ran.append(1), ttl=5, wait=0.3) is True
    assert ran == [1] and r.exists("t:job") == 0


def test_run_exclusive_reports_an_overrun(r):
    with pytest.raises(LockNotOwnedError):
        run_exclusive(r, "t:job", lambda: time.sleep(0.5), ttl=0.3, wait=1)


def test_extend_needs_a_timeout_and_replace_ttl_resets_it(r):
    lock = r.lock("t:job", timeout=1)
    assert lock.acquire()
    lock.extend(5, replace_ttl=True)
    assert 4000 < r.pttl("t:job") <= 5000
    forever = r.lock("t:forever")
    assert forever.acquire()
    with pytest.raises(LockError, match="no timeout"):
        forever.extend(5)
    forever.release()


def test_thread_local_stops_a_thread_releasing_another_threads_lock(r):
    shared = r.lock("t:job", timeout=0.3)  # one Lock object used by two threads
    assert shared.acquire()
    time.sleep(0.4)  # expired
    got = []
    t = threading.Thread(target=lambda: got.append(shared.acquire(blocking=False)))
    t.start()
    t.join()
    assert got == [True]  # thread 2 holds it now, with its own token
    with pytest.raises(LockNotOwnedError):
        shared.release()  # thread 1 still has its old token, so it can't delete thread 2's lock
    assert r.exists("t:job") == 1


def test_thread_local_does_not_separate_asyncio_tasks():
    async def main() -> bool:
        ar = aredis.Redis.from_url(REDIS_URL)
        shared = ar.lock("t:job", timeout=0.3)  # one Lock object used by two tasks
        assert await shared.acquire()
        await asyncio.sleep(0.4)  # task 1's lease expired
        assert await asyncio.create_task(shared.acquire(blocking=False))  # task 2 takes it
        await shared.release()  # task 1 releases... using task 2's token
        still_locked = await ar.exists("t:job")
        await ar.delete("t:job")
        await ar.aclose()
        return bool(still_locked)

    assert asyncio.run(main()) is False  # task 2's lock was deleted: one Lock object per task


def test_async_run_exclusive_skips_runs_and_reports_overrun():
    async def main():
        ar = aredis.Redis.from_url(REDIS_URL)
        ran = []

        async def job():
            ran.append(1)

        async def slow_job():
            await asyncio.sleep(0.5)

        try:
            assert await run_exclusive_async(ar, "t:ajob", job, ttl=5, wait=0.3) is True
            held = ar.lock("t:ajob", timeout=5)
            assert await held.acquire()
            assert await run_exclusive_async(ar, "t:ajob", job, ttl=5, wait=0.3) is False
            await held.release()
            with pytest.raises(LockNotOwnedError):
                await run_exclusive_async(ar, "t:ajob", slow_job, ttl=0.3, wait=1)
            assert ran == [1]
        finally:
            await ar.delete("t:ajob")
            await ar.aclose()

    asyncio.run(main())


def test_lock_class_is_the_same_on_the_asyncio_client():
    ar = aredis.Redis.from_url(REDIS_URL)
    lock = ar.lock("t:x")
    assert (lock.timeout, lock.blocking_timeout, lock.sleep, lock.thread_local) == (None, None, 0.1, True)
    assert not isinstance(lock, Lock)  # a separate class, redis.asyncio.lock.Lock, same API
    asyncio.run(ar.aclose())
