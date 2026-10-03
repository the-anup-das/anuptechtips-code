"""The two other shapes of a cancellation bug: a slot that never comes back, and a cancel
that gets swallowed."""
import asyncio
import time

import pytest

from helpers import cancel_mid_flight, held_toy, user_id_of
from stuck_slot import query_leaky
from swallowed_cancel import POOL_TIMEOUT, getconn, getconn_swallowing
from toy_pool import query


def test_except_exception_leaks_a_slot_per_cancel_until_the_pool_is_stuck():
    async def main():
        async with held_toy(size=2) as (server, pool):
            await cancel_mid_flight(server, pool, query_leaky, "user:1")
            await cancel_mid_flight(server, pool, query_leaky, "user:2")
            server.hold.set()  # the server is healthy and answering
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(query_leaky(pool, "user:3"), 0.3)

    asyncio.run(main())  # two cancels, two slots gone, a pool of two has nothing to give


def test_except_base_exception_gives_the_slot_back():
    async def main():
        async with held_toy(size=2) as (server, pool):
            await cancel_mid_flight(server, pool, query, "user:1")
            await cancel_mid_flight(server, pool, query, "user:2")
            server.hold.set()
            return user_id_of(await asyncio.wait_for(query(pool, "user:3"), 0.3))

    assert asyncio.run(main()) == 3


def test_except_exception_is_fine_until_a_cancel_arrives():
    async def main():
        async with held_toy(size=1) as (server, pool):
            server.hold.set()
            return [user_id_of(await query_leaky(pool, f"user:{n}")) for n in (1, 2, 3)]

    assert asyncio.run(main()) == [1, 2, 3]  # which is why the happy-path tests pass


def waited(getconn_fn) -> float:
    """Seconds a caller with a 50 ms budget waits for a connection from a busy pool."""
    async def main():
        async with held_toy(size=1) as (server, pool):
            busy = await pool.acquire()  # the only connection is taken
            started = time.perf_counter()
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(getconn_fn(pool), 0.05)
            elapsed = time.perf_counter() - started
            pool.release(busy)
            return elapsed

    return asyncio.run(main())


def test_a_swallowed_cancel_turns_a_50_ms_budget_into_the_pools_own_timeout():
    assert waited(getconn_swallowing) >= POOL_TIMEOUT * 0.9


def test_a_re_raised_cancel_keeps_the_callers_budget():
    assert waited(getconn) < 0.3


def test_cancelled_error_is_not_an_exception():
    assert issubclass(asyncio.CancelledError, BaseException)
    assert not issubclass(asyncio.CancelledError, Exception)
