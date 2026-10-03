"""Exact-match response cache: the same request from the same tenant gets the stored answer."""
import hashlib
import json

import redis.asyncio as redis

from routes import Route

TRANSPORT_ONLY = {"stream", "stream_options", "user"}  # fields that don't change the answer


def cache_key(tenant: str, route: Route, body: dict) -> str:
    """Everything that can change the answer goes into the key: who is asking, which
    models may serve it, and the whole request (system prompt, messages, tools, params)."""
    request = {k: v for k, v in body.items() if k not in TRANSPORT_ONLY}
    material = [tenant, [target.name for target in route.chain], request]
    digest = hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()
    return f"cache:{tenant}:{digest}"


class Cache:
    def __init__(self, r: redis.Redis) -> None:
        self.r = r

    async def get(self, key: str) -> dict | None:
        stored = await self.r.get(key)
        return json.loads(stored) if stored else None

    async def put(self, key: str, response: dict, ttl_s: int) -> None:
        # A cached answer is user data at rest: it gets a TTL, like the logs.
        await self.r.set(key, json.dumps(response), ex=ttl_s)
