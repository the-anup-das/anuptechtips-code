"""Reserve, then settle: the budget under concurrency, retries, crashes and a hierarchy."""
import asyncio

import pytest
from redis.crc import key_slot

import budget as budget_module
from budget import Budget, limit_key, spent_key
from naive_budget import NaiveBudget

pytestmark = pytest.mark.anyio
ORG = "demo"
LEVELS = ["org", "team:support", "tenant:acme", "key:vk_refunds"]


async def spent(b: Budget) -> list[int]:
    return [await b.spent(ORG, level) for level in LEVELS]


async def test_reserve_holds_the_worst_case_and_settle_refunds_the_rest(r):
    b = Budget(r)
    await b.set_limit(ORG, "tenant:acme", 10_000)
    resv = await b.reserve(ORG, LEVELS, "req-1", 4_000)  # input + max_tokens, priced
    assert await spent(b) == [4_000] * 4                 # held at every level
    assert await b.settle(resv, 1_500) == 2_500          # the call used less: refund
    assert await spent(b) == [1_500] * 4


async def test_200_concurrent_reservations_against_a_budget_for_10(r):
    b = Budget(r)
    await b.set_limit(ORG, "tenant:acme", 10 * 4_000)
    results = await asyncio.gather(*(b.reserve(ORG, LEVELS, f"req-{i}", 4_000)
                                     for i in range(200)))
    assert sum(resv is not None for resv in results) == 10  # exactly 10, never 11
    assert await b.spent(ORG, "tenant:acme") == 40_000


async def test_check_then_spend_lets_all_200_through(r):
    """The racing version: nothing is written until the model call returns. Here every
    request has passed the check before the first call comes back, as in a real burst
    against a model that takes seconds (measure_budget_race.py does it over HTTP)."""
    lua, naive = Budget(r), NaiveBudget(r)
    await lua.set_limit(ORG, "tenant:acme", 10 * 4_000)
    checked, all_checked = 0, asyncio.Event()

    async def request(i: int) -> bool:
        nonlocal checked
        resv = await naive.reserve(ORG, LEVELS, f"req-{i}", 4_000)
        if resv is None:
            return False
        checked += 1
        if checked == 200:
            all_checked.set()
        await asyncio.wait_for(all_checked.wait(), timeout=10)  # the model call
        await naive.settle(resv, 4_000)
        return True

    passed = sum(await asyncio.gather(*(request(i) for i in range(200))))
    assert passed == 200
    assert await lua.spent(ORG, "tenant:acme") == 800_000  # 20 times the budget


async def test_a_second_settle_is_a_no_op(r):
    b = Budget(r)
    resv = await b.reserve(ORG, LEVELS, "req-1", 4_000)
    assert await b.settle(resv, 1_500) == 2_500
    assert await b.settle(resv, 1_500) == -1  # nothing left to settle
    assert await spent(b) == [1_500] * 4      # and nothing was refunded twice


async def test_an_expired_reservation_stays_charged(r, monkeypatch):
    monkeypatch.setattr(budget_module, "RESERVATION_TTL_S", 1)
    b = Budget(r)
    resv = await b.reserve(ORG, LEVELS, "req-1", 4_000)
    await asyncio.sleep(1.2)  # the gateway died before it could settle
    assert await r.exists(resv.key) == 0
    assert await spent(b) == [4_000] * 4      # the worst case is still counted: fails safe
    assert await b.settle(resv, 1_500) == -1  # a late settle can't refund what it can't see


async def test_the_hierarchy_is_all_or_nothing(r):
    b = Budget(r)
    await b.set_limit(ORG, "org", 1_000_000)
    await b.set_limit(ORG, "team:support", 5_000)  # the team is the tight one
    assert await b.reserve(ORG, LEVELS, "req-1", 4_000) is not None
    assert await b.reserve(ORG, LEVELS, "req-2", 4_000) is None
    assert await spent(b) == [4_000] * 4  # the refused request debited no level, not even org


async def test_the_same_reservation_id_is_debited_once(r):
    b = Budget(r)
    first = await b.reserve(ORG, LEVELS, "idem-key-1", 4_000)
    again = await b.reserve(ORG, LEVELS, "idem-key-1", 4_000)  # the client's retry
    assert first is not None and again is not None
    assert await spent(b) == [4_000] * 4
    await b.settle(again, 1_000)
    assert await spent(b) == [1_000] * 4


async def test_a_level_without_a_limit_is_counted_but_never_refuses(r):
    b = Budget(r)
    assert await b.reserve(ORG, LEVELS, "req-1", 10**12) is not None
    assert await spent(b) == [10**12] * 4


async def test_actual_cost_above_the_reservation_is_still_charged(r):
    b = Budget(r)
    resv = await b.reserve(ORG, LEVELS, "req-1", 1_000)
    assert await b.settle(resv, 1_200) == -200  # the estimate was low: charge the difference
    assert await spent(b) == [1_200] * 4


async def test_one_orgs_keys_share_a_cluster_slot(r):
    b = Budget(r)
    resv = await b.reserve(ORG, LEVELS, "req-1", 1)
    keys = [resv.key, *resv.spent_keys, *(limit_key(ORG, level) for level in LEVELS)]
    assert len({key_slot(key.encode()) for key in keys}) == 1  # the {demo} hash tag
    assert key_slot(spent_key("other-org", "org").encode()) != key_slot(keys[0].encode())
