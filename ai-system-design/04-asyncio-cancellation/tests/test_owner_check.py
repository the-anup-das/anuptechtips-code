"""The owner check: a cached value has to prove whose it is before anyone sees it."""
import asyncio
import json
import logging

import pytest
import redis.asyncio as redis

from helpers import REDIS_DB, REDIS_HOST, REDIS_PORT
from owner_check import get_profile, metrics, owned_by, set_profile


def with_client(coro_fn):
    async def main():
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        try:
            return await coro_fn(client)
        finally:
            await client.aclose()

    metrics.clear()
    return asyncio.run(main())


def test_the_owner_gets_the_profile(db):
    async def scenario(r):
        await set_profile(r, 7, {"email": "seven@example.com"})
        return await get_profile(r, 7)

    assert with_client(scenario) == {"email": "seven@example.com", "user_id": 7}
    assert metrics["cache_owner_mismatch"] == 0


def test_set_profile_stamps_the_owner_even_over_a_wrong_one(db):
    async def scenario(r):
        await set_profile(r, 7, {"user_id": 99, "email": "seven@example.com"})
        return await get_profile(r, 7)

    assert with_client(scenario)["user_id"] == 7


def test_an_ordinary_miss_is_not_counted(db):
    assert with_client(lambda r: get_profile(r, 404)) is None
    assert metrics["cache_owner_mismatch"] == 0


def test_a_wrong_owner_value_is_a_miss_and_a_count_of_one(db, caplog):
    db.set("user:2", json.dumps({"user_id": 1, "email": "one@example.com"}))  # user 1's data
    with caplog.at_level(logging.ERROR):
        assert with_client(lambda r: get_profile(r, 2)) is None
    assert metrics["cache_owner_mismatch"] == 1
    assert "user:2" in caplog.text
    assert "one@example.com" not in caplog.text  # the leaked value never reaches the log


@pytest.mark.parametrize("stale", [
    b"OK",                                # the reply to a SELECT or a SET
    b"42",                                # a counter
    b"[1, 2]",                            # a list
    json.dumps({"email": "nobody@example.com"}).encode(),  # a dict with no owner
    json.dumps({"user_id": "2"}).encode(),  # the right id as the wrong type
])
def test_a_value_that_cannot_prove_its_owner_is_a_counted_miss(db, stale):
    db.set("user:2", stale)
    assert with_client(lambda r: get_profile(r, 2)) is None
    assert metrics["cache_owner_mismatch"] == 1


def test_owned_by_handles_replies_that_are_not_strings():
    assert owned_by(1, 2) is None        # an integer reply, as from EXISTS
    assert owned_by(None, 2) is None
    assert owned_by([b"a"], 2) is None   # a list reply
    assert owned_by(json.dumps({"user_id": 2}), 2) == {"user_id": 2}
