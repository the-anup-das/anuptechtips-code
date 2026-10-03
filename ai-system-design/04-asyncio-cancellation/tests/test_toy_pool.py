"""The toy pool: what a cancel between "sent" and "reply read" does, and what fixes it."""
import asyncio

import pytest

from helpers import cancel_mid_flight, held_toy, user_id_of
from toy_pool import Pool, query, query_buggy


def test_buggy_pool_hands_the_next_caller_the_cancelled_reply():
    async def main():
        async with held_toy() as (server, pool):
            await cancel_mid_flight(server, pool, query_buggy, "user:1")
            server.hold.set()  # the server now answers everything it received, in order
            return user_id_of(await query_buggy(pool, "user:2")), server.connections

    assert asyncio.run(main()) == (1, 1)  # asked for user 2, got user 1, same connection


def test_buggy_pool_stays_off_by_one():
    async def main():
        async with held_toy() as (server, pool):
            await cancel_mid_flight(server, pool, query_buggy, "user:1")
            server.hold.set()
            return [user_id_of(await query_buggy(pool, f"user:{n}")) for n in (2, 3, 4, 5)]

    assert asyncio.run(main()) == [1, 2, 3, 4]  # every caller gets the previous caller's data


def test_fixed_pool_closes_the_cancelled_connection():
    async def main():
        async with held_toy() as (server, pool):
            await cancel_mid_flight(server, pool, query, "user:1")
            server.hold.set()
            replies = [user_id_of(await query(pool, f"user:{n}")) for n in (2, 3, 4, 5)]
            return replies, server.connections

    assert asyncio.run(main()) == ([2, 3, 4, 5], 2)  # own data, on one fresh connection


def test_fixed_pool_reuses_a_clean_connection():
    async def main():
        async with held_toy() as (server, pool):
            server.hold.set()
            for n in range(5):
                assert user_id_of(await query(pool, f"user:{n}")) == n
            return server.connections

    assert asyncio.run(main()) == 1


async def by_task_cancel(server, pool, query_fn):
    await cancel_mid_flight(server, pool, query_fn, "user:1")


async def by_asyncio_timeout(server, pool, query_fn):
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await query_fn(pool, "user:1")


async def by_wait_for(server, pool, query_fn):
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(query_fn(pool, "user:1"), 0.05)


async def by_task_group_sibling(server, pool, query_fn):
    async def fail_once_the_request_is_sent():
        await server.received.get()
        raise RuntimeError("a sibling task failed")

    with pytest.raises(ExceptionGroup):
        async with asyncio.TaskGroup() as group:
            group.create_task(query_fn(pool, "user:1"))
            group.create_task(fail_once_the_request_is_sent())


CANCEL_SOURCES = [by_task_cancel, by_asyncio_timeout, by_wait_for, by_task_group_sibling]


@pytest.mark.parametrize("cancel", CANCEL_SOURCES)
def test_every_kind_of_cancel_poisons_the_buggy_pool(cancel):
    async def main():
        async with held_toy() as (server, pool):
            await cancel(server, pool, query_buggy)
            server.hold.set()
            return user_id_of(await query_buggy(pool, "user:2"))

    assert asyncio.run(main()) == 1


@pytest.mark.parametrize("cancel", CANCEL_SOURCES)
def test_no_kind_of_cancel_poisons_the_fixed_pool(cancel):
    async def main():
        async with held_toy() as (server, pool):
            await cancel(server, pool, query)
            server.hold.set()
            return user_id_of(await query(pool, "user:2"))

    assert asyncio.run(main()) == 2


@pytest.mark.parametrize("query_fn", [query_buggy, query])
def test_a_cancel_while_waiting_for_a_slot_is_harmless(query_fn):
    """Nothing was sent, so there is no reply to leave behind."""
    async def main():
        async with held_toy(size=1) as (server, pool):
            first = asyncio.create_task(query_fn(pool, "user:1"))
            await server.received.get()  # the only connection is busy with user 1
            waiting = asyncio.create_task(query_fn(pool, "user:2"))
            await asyncio.sleep(0.01)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            server.hold.set()
            replies = [user_id_of(await first), user_id_of(await query_fn(pool, "user:3"))]
            return replies, server.connections

    assert asyncio.run(main()) == ([1, 3], 1)


def test_fixed_pool_keeps_its_slots_after_many_cancels():
    async def main():
        async with held_toy(size=2) as (server, pool):
            for n in range(6):
                await cancel_mid_flight(server, pool, query, f"user:{n}")
            server.hold.set()
            async with asyncio.timeout(2):  # both slots are still there to hand out
                replies = await asyncio.gather(query(pool, "user:7"), query(pool, "user:8"))
            return sorted(user_id_of(reply) for reply in replies)

    assert asyncio.run(main()) == [7, 8]


def test_fixed_pool_drops_a_connection_the_server_closed():
    async def main():
        accepted = 0

        async def hang_up(reader, writer):
            nonlocal accepted
            accepted += 1
            await reader.readline()
            writer.close()

        server = await asyncio.start_server(hang_up, "127.0.0.1", 0)
        pool = Pool("127.0.0.1", server.sockets[0].getsockname()[1], size=1)
        for _ in range(2):
            with pytest.raises(asyncio.IncompleteReadError):
                async with asyncio.timeout(2):  # the slot came back, so this can't hang
                    await query(pool, "user:1")
        server.close()
        return accepted, len(pool.idle)

    assert asyncio.run(main()) == (2, 0)  # a new connection each time, nothing kept
