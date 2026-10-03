"""Idempotency-Key for the gateway: a client's retry gets the first answer back
instead of a second model call and a second charge."""
import hashlib
import json

import redis.asyncio as redis

LEASE_S = 120       # an in-flight claim outlives the request deadline, then frees itself
KEEP_S = 24 * 3600  # how long a finished answer is replayed


class Replay(Exception):
    """The key already has a finished answer: send that instead of calling a model."""

    def __init__(self, status: int, body: dict) -> None:
        self.status, self.body = status, body


class Conflict(Exception):
    """The key can't be served: 409 while it is in flight, 422 for a different body."""

    def __init__(self, status: int, code: str, message: str) -> None:
        self.status, self.code, self.message = status, code, message


class Claim:
    def __init__(self, r: redis.Redis, key: str, fingerprint: str) -> None:
        self.r, self.key, self.fingerprint = r, key, fingerprint

    async def complete(self, status: int, body: dict) -> None:
        record = {"state": "done", "fp": self.fingerprint, "status": status, "body": body}
        await self.r.set(self.key, json.dumps(record), ex=KEEP_S)

    async def streamed(self) -> None:
        # A stream isn't stored, so it can't be replayed. Remember that it ran.
        record = {"state": "streamed", "fp": self.fingerprint}
        await self.r.set(self.key, json.dumps(record), ex=KEEP_S)

    async def release(self) -> None:
        await self.r.delete(self.key)  # nothing was produced: the client may try again


async def claim(r: redis.Redis, tenant: str, key: str, body: dict) -> Claim:
    """Claim the key for this request, or raise Replay or Conflict."""
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    rkey = f"idem:{tenant}:{key}"
    mine = json.dumps({"state": "in_flight", "fp": fingerprint})
    # SET NX is the claim; GET returns what was already there, in the same round trip.
    existing = await r.set(rkey, mine, nx=True, ex=LEASE_S, get=True)
    if existing is None:
        return Claim(r, rkey, fingerprint)
    record = json.loads(existing)
    if record["fp"] != fingerprint:
        raise Conflict(422, "idempotency_key_reused",
                       "this Idempotency-Key was used with a different request body")
    if record["state"] == "in_flight":
        raise Conflict(409, "idempotency_key_in_flight",
                       "a request with this Idempotency-Key is still running")
    if record["state"] == "streamed":
        raise Conflict(409, "stream_not_replayable",
                       "this Idempotency-Key ran as a stream, and streams aren't stored")
    raise Replay(record["status"], record["body"])
