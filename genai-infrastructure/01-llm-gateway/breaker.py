"""A circuit breaker whose state lives in Redis, so every gateway replica agrees on it."""
from typing import NamedTuple

import redis.asyncio as redis


class Gate(NamedTuple):
    state: str     # "closed": go ahead. "open": skip this provider. "probe": go, you're the test
    failures: int  # failures counted since the last success


class Breaker:
    def __init__(self, r: redis.Redis, threshold: int = 3, window_s: int = 30,
                 open_s: float = 10.0, max_open_s: float = 600.0, probe_ms: int = 5000) -> None:
        self.r, self.threshold, self.window_s = r, threshold, window_s
        self.open_s, self.max_open_s, self.probe_ms = open_s, max_open_s, probe_ms

    @staticmethod
    def _key(name: str, part: str) -> str:
        return f"cb:{{{name}}}:{part}"  # {name} keeps one provider's keys in one Cluster slot

    async def allow(self, name: str) -> Gate:
        opened, half, fails = await self.r.mget(
            self._key(name, "open"), self._key(name, "half"), self._key(name, "fails"))
        if opened:
            return Gate("open", 0)
        if not half:
            return Gate("closed", int(fails or 0))
        # Half-open: the open time has run out and nobody has seen a success yet. One
        # request across all replicas may try. SET NX is the lock, and its TTL frees it
        # if the prober dies. A lock lost that way costs one extra probe, nothing worse.
        won = await self.r.set(self._key(name, "probe"), "1", nx=True, px=self.probe_ms)
        return Gate("probe" if won else "open", 0)

    async def success(self, name: str, gate: Gate) -> None:
        if gate.state == "probe" or gate.failures:
            await self.r.delete(self._key(name, "half"), self._key(name, "fails"),
                                self._key(name, "probe"))

    async def failure(self, name: str, gate: Gate, open_for: float | None = None) -> None:
        """Count one failure. Trip at the threshold, on a failed probe, or at once when
        the provider said how long to stay away (open_for, taken from Retry-After)."""
        fails = await self.r.incr(self._key(name, "fails"))
        await self.r.expire(self._key(name, "fails"), self.window_s)
        if open_for is None and gate.state != "probe" and fails < self.threshold:
            return
        seconds = min(open_for or self.open_s, self.max_open_s)
        pipe = self.r.pipeline()
        pipe.set(self._key(name, "open"), "1", px=max(1, int(seconds * 1000)))
        pipe.set(self._key(name, "half"), "1")
        pipe.delete(self._key(name, "fails"), self._key(name, "probe"))
        await pipe.execute()

    async def retry_after(self, name: str) -> float:
        """Seconds until the breaker lets a probe through (0 when it isn't open)."""
        return max(0, await self.r.pttl(self._key(name, "open"))) / 1000


class NoBreaker:
    """Breaker switched off: every call goes through."""

    async def allow(self, name: str) -> Gate:
        return Gate("closed", 0)

    async def success(self, name: str, gate: Gate) -> None:
        pass

    async def failure(self, name: str, gate: Gate, open_for: float | None = None) -> None:
        pass

    async def retry_after(self, name: str) -> float:
        return 0.0
