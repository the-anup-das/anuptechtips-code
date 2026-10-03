"""The circuit breaker: closed, open, half-open, with its state shared through Redis."""
import asyncio

import pytest

from breaker import Breaker, Gate

pytestmark = pytest.mark.anyio
NAME = "alpha/alpha-large"


async def trip(breaker: Breaker, **kw) -> None:
    for _ in range(breaker.threshold):
        await breaker.failure(NAME, await breaker.allow(NAME), **kw)


async def test_closed_until_three_failures_in_a_row(r):
    breaker = Breaker(r)
    for _ in range(2):
        await breaker.failure(NAME, await breaker.allow(NAME))
    assert (await breaker.allow(NAME)) == Gate("closed", 2)
    await breaker.failure(NAME, await breaker.allow(NAME))
    assert (await breaker.allow(NAME)).state == "open"


async def test_a_success_resets_the_count(r):
    breaker = Breaker(r)
    for _ in range(3):  # fail, fail, succeed: never three in a row
        await breaker.failure(NAME, await breaker.allow(NAME))
        await breaker.failure(NAME, await breaker.allow(NAME))
        await breaker.success(NAME, await breaker.allow(NAME))
    assert (await breaker.allow(NAME)) == Gate("closed", 0)


async def test_open_time_comes_from_retry_after(r):
    breaker = Breaker(r, open_s=60)
    # One failure that carries the provider's Retry-After trips at once, for that long.
    await breaker.failure(NAME, await breaker.allow(NAME), open_for=0.3)
    assert (await breaker.allow(NAME)).state == "open"
    assert 0 < await breaker.retry_after(NAME) <= 0.3
    await asyncio.sleep(0.35)
    assert (await breaker.allow(NAME)).state == "probe"  # not 60 s: the provider said 0.3


async def test_open_time_is_capped(r):
    breaker = Breaker(r, max_open_s=2)
    await breaker.failure(NAME, await breaker.allow(NAME), open_for=3600)
    assert await breaker.retry_after(NAME) <= 2


async def test_half_open_lets_one_probe_through_across_replicas(r):
    replica_a, replica_b = Breaker(r, open_s=0.1), Breaker(r, open_s=0.1)
    await trip(replica_a)
    await asyncio.sleep(0.15)  # the open time has run out: half-open
    gates = await asyncio.gather(*(b.allow(NAME) for b in [replica_a, replica_b] * 10))
    assert sorted(g.state for g in gates).count("probe") == 1
    assert sorted(g.state for g in gates).count("open") == 19


async def test_a_successful_probe_closes_the_breaker(r):
    breaker = Breaker(r, open_s=0.1)
    await trip(breaker)
    await asyncio.sleep(0.15)
    probe = await breaker.allow(NAME)
    assert probe.state == "probe"
    await breaker.success(NAME, probe)
    assert (await breaker.allow(NAME)) == Gate("closed", 0)


async def test_a_failed_probe_reopens_it_at_once(r):
    breaker = Breaker(r, open_s=0.1)
    await trip(breaker)
    await asyncio.sleep(0.15)
    probe = await breaker.allow(NAME)
    await breaker.failure(NAME, probe)  # one failure is enough in half-open
    assert (await breaker.allow(NAME)).state == "open"


async def test_a_lost_probe_lock_costs_one_extra_probe(r):
    breaker = Breaker(r, open_s=0.1, probe_ms=200)
    await trip(breaker)
    await asyncio.sleep(0.15)
    assert (await breaker.allow(NAME)).state == "probe"  # this prober then dies, or is slow
    assert (await breaker.allow(NAME)).state == "open"
    await asyncio.sleep(0.25)                            # its lock expires
    assert (await breaker.allow(NAME)).state == "probe"  # one more request gets to try


async def test_breakers_are_per_target(r):
    breaker = Breaker(r)
    await trip(breaker)
    assert (await breaker.allow("beta/beta-large")).state == "closed"
