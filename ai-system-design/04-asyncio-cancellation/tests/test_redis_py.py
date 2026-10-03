"""The installed redis-py (4.5.5 or later): a cancel after the send closes the connection."""
import asyncio
import contextlib
import time

import pytest
import redis.asyncio as redis

from delay_proxy import DelayProxy
from helpers import REDIS_DB, REDIS_HOST, REDIS_PORT

DELAY = 0.1  # seconds each way, as in redis-py issue #2665


@contextlib.asynccontextmanager
async def slow_redis(max_connections: int = 1):
    """A redis-py client whose every byte takes DELAY seconds to cross a proxy."""
    proxy = DelayProxy(REDIS_HOST, REDIS_PORT, DELAY)
    pool = redis.BlockingConnectionPool(host="127.0.0.1", port=await proxy.start(), db=REDIS_DB,
                                        max_connections=max_connections, timeout=None)
    client = redis.Redis(connection_pool=pool)
    try:
        yield client, proxy
    finally:
        await client.aclose()
        await pool.disconnect()
        proxy.close()
        await asyncio.sleep(0.01)


def test_the_proxy_delays_both_directions(db):
    async def main():
        async with slow_redis() as (r, proxy):
            await r.ping()  # connect and handshake first
            started = time.perf_counter()
            await r.ping()
            return time.perf_counter() - started, proxy.connections

    elapsed, connections = asyncio.run(main())
    assert elapsed >= 2 * DELAY
    assert connections == 1


def test_a_cancelled_get_does_not_leak_into_the_next_one(db):
    async def main():
        async with slow_redis() as (r, proxy):
            await r.set("user:1", "data of user 1")
            await r.set("user:2", "data of user 2")
            task = asyncio.create_task(r.get("user:1"))
            await asyncio.sleep(DELAY / 2)  # sent, and the reply is still on the wire
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return await r.get("user:2"), await r.get("user:1"), proxy.connections

    assert asyncio.run(main()) == (b"data of user 2", b"data of user 1", 2)  # it reconnected


def test_asyncio_timeout_around_a_get_is_safe(db):
    async def main():
        async with slow_redis() as (r, proxy):
            await r.set("user:1", "data of user 1")
            await r.set("user:2", "data of user 2")
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(DELAY / 2):
                    await r.get("user:1")
            return await r.get("user:2"), proxy.connections

    assert asyncio.run(main()) == (b"data of user 2", 2)


def test_a_cancelled_pipeline_does_not_leak_either(db):
    """The CVE-2023-28858 case: the cancel lands while a pipeline's replies are pending."""
    async def main():
        async with slow_redis() as (r, proxy):
            await r.mset({"user:1": "data of user 1", "user:2": "data of user 2"})
            pipe = r.pipeline(transaction=False)
            pipe.get("user:1")
            pipe.get("user:1")
            task = asyncio.create_task(pipe.execute())
            await asyncio.sleep(DELAY / 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return await r.get("user:2")

    assert asyncio.run(main()) == b"data of user 2"


def test_cancelled_reads_do_not_use_up_the_pool(db):
    async def main():
        async with slow_redis(max_connections=2) as (r, proxy):
            await r.mset({"user:1": "data of user 1", "user:2": "data of user 2"})
            for _ in range(6):
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(DELAY / 2):
                        await r.get("user:1")
            async with asyncio.timeout(5):  # both connections are still available
                return await asyncio.gather(r.get("user:1"), r.get("user:2"))

    assert asyncio.run(main()) == [b"data of user 1", b"data of user 2"]
