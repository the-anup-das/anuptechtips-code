"""Make a cached value prove whose it is: the owner's id is stored inside the value and
checked on every read. A value that fails the check is counted and treated as a miss."""
import json
import logging
from collections import Counter

import redis.asyncio as redis

log = logging.getLogger(__name__)
metrics: Counter[str] = Counter()  # stand-in for your metrics client


async def set_profile(r: redis.Redis, user_id: int, profile: dict) -> None:
    await r.set(f"user:{user_id}", json.dumps({**profile, "user_id": user_id}))


def owned_by(raw: object, user_id: int) -> dict | None:
    """The decoded value if it is a profile that belongs to `user_id`, else None."""
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):  # a reply of another type, or not JSON at all
        return None
    if isinstance(value, dict) and value.get("user_id") == user_id:
        return value
    return None


async def get_profile(r: redis.Redis, user_id: int) -> dict | None:
    raw = await r.get(f"user:{user_id}")
    if raw is None:
        return None  # an ordinary miss: load from the database
    profile = owned_by(raw, user_id)
    if profile is None:
        metrics["cache_owner_mismatch"] += 1  # alert on this: it should stay at zero
        log.error("cache owner mismatch on user:%s (value not logged)", user_id)
    return profile  # None is a miss, so the caller never sees someone else's data
