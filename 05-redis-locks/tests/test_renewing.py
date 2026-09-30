import asyncio
import subprocess
import sys
import time

import pytest
import redis.asyncio as aredis
from redis.exceptions import LockError

from conftest import HERE, REDIS_URL
from procfreeze import freeze, thaw
from renewing import renewing_lock, renewing_lock_async


def test_renewal_outlives_the_ttl_while_the_work_is_slow(r):
    with renewing_lock(r, "t:job", ttl=0.3, wait=1) as lost:
        time.sleep(1.0)  # more than 3 TTLs
        assert r.exists("t:job") == 1 and not lost.is_set()
    assert r.exists("t:job") == 0  # released on exit


def test_a_hung_worker_keeps_its_lock_alive(r):
    """The flip side: a thread renewer keeps renewing while the work is stuck."""
    with renewing_lock(r, "t:job", ttl=0.3, wait=1):
        time.sleep(1.5)  # "hung" for 5 TTLs; nobody else can take the job
        assert not r.lock("t:job", timeout=1).acquire(blocking=False)


def test_lost_is_set_when_someone_else_takes_the_lock(r):
    with renewing_lock(r, "t:job", ttl=0.3, wait=1) as lost:
        r.delete("t:job")
        r.set("t:job", "someone-else", px=5000)  # e.g. after an expiry we didn't see
        time.sleep(0.25)
        assert lost.is_set()
    assert r.get("t:job") == b"someone-else"  # we didn't delete their lock on exit


def test_busy_lock_raises(r):
    assert r.lock("t:job", timeout=5).acquire()
    with pytest.raises(LockError, match="busy"):
        with renewing_lock(r, "t:job", ttl=1, wait=0.2):
            pass


def test_freezing_the_process_freezes_the_renewer_and_the_lock_expires(r):
    holder = subprocess.Popen(
        [sys.executable, str(HERE / "renewing.py"), REDIS_URL, "t:frozen", "1.0", "6"],
        stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "acquired"
        time.sleep(1.5)
        assert r.exists("t:frozen") == 1  # renewal works while the process runs
        freeze(holder.pid)                # like kill -STOP or docker pause
        time.sleep(2.0)                   # two TTLs with every thread stopped
        assert r.exists("t:frozen") == 0  # the lease ran out
        assert r.set("t:frozen", "worker-B", nx=True, px=10_000)  # another worker takes it
        thaw(holder.pid)
        assert holder.stdout.readline().strip() == "lost"  # A only finds out after it wakes
        assert holder.wait(timeout=5) == 3
        assert r.get("t:frozen") == b"worker-B"
    finally:
        if holder.poll() is None:
            thaw(holder.pid)
            holder.kill()


def test_asyncio_blocking_call_starves_renewal():
    async def main():
        ar = aredis.Redis.from_url(REDIS_URL)
        try:
            async with renewing_lock_async(ar, "t:ajob", ttl=0.3, wait=1) as lost:
                time.sleep(0.9)  # a blocking call: the event loop can't run the renewer
                assert await ar.exists("t:ajob") == 0  # expired while we were "holding" it
                await asyncio.sleep(0.2)  # the renewer finally runs, and finds out
                assert lost.is_set()
        finally:
            await ar.delete("t:ajob")
            await ar.aclose()

    asyncio.run(main())


def test_asyncio_renewal_works_when_the_loop_is_free():
    async def main():
        ar = aredis.Redis.from_url(REDIS_URL)
        try:
            async with renewing_lock_async(ar, "t:ajob", ttl=0.3, wait=1) as lost:
                await asyncio.sleep(0.9)
                assert await ar.exists("t:ajob") == 1 and not lost.is_set()
            assert await ar.exists("t:ajob") == 0
        finally:
            await ar.aclose()

    asyncio.run(main())
