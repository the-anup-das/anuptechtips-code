"""The same flag, cached in-process: one Redis read per `ttl` seconds instead of one per
request. The price is that a rollback takes up to `ttl` to reach this process."""
import time

import redis

import rollout


class CachedVersions:
    def __init__(self, r: redis.Redis, ttl: float = 1.0):
        self.r, self.ttl = r, ttl
        self.flag: tuple[str, str, int] | None = None
        self.read_at = 0.0

    def version_for(self, user_id: str) -> str:
        now = time.monotonic()
        if self.flag is None or now - self.read_at >= self.ttl:
            stable, candidate, percent = self.r.hmget(rollout.KEY, "stable", "candidate", "percent")
            self.flag, self.read_at = (stable.decode(), candidate.decode(), int(percent)), now
        stable, candidate, percent = self.flag
        return candidate if rollout.cohort(user_id) < percent else stable
