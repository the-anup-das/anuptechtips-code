"""Backpressure: cap what is in flight, per tenant and per provider, and say no early."""
import asyncio
import collections

import anyio


class Admission:
    """In-flight limits for one gateway process. With N replicas, give each one 1/N of
    the real limit, or move these counters into Redis."""

    def __init__(self, per_tenant: int, per_provider: int, queue_wait_s: float,
                 max_queue: int = 100) -> None:
        self.per_tenant, self.queue_wait_s, self.max_queue = per_tenant, queue_wait_s, max_queue
        self.tenants: collections.Counter[str] = collections.Counter()
        self.waiting: collections.Counter[str] = collections.Counter()
        self.slots: dict[str, asyncio.Semaphore] = collections.defaultdict(
            lambda: asyncio.Semaphore(per_provider))

    def enter(self, tenant: str) -> bool:
        """One more request for this tenant, unless it is at its limit. There is no await
        between the check and the increment, so two requests can't both take the last slot."""
        if self.tenants[tenant] >= self.per_tenant:
            return False
        self.tenants[tenant] += 1
        return True

    def leave(self, tenant: str) -> None:
        self.tenants[tenant] -= 1

    async def admit(self, provider: str) -> bool:
        """Take one of the provider's slots, waiting at most queue_wait_s for it.
        False means the provider is saturated: fail over or shed the request."""
        slots = self.slots[provider]
        if slots.locked() and self.waiting[provider] >= self.max_queue:
            return False  # the queue is full: reject now instead of joining it
        self.waiting[provider] += 1
        try:
            with anyio.fail_after(self.queue_wait_s):
                await slots.acquire()
            return True
        except TimeoutError:
            return False  # waited long enough
        finally:
            self.waiting[provider] -= 1

    def release(self, provider: str) -> None:
        self.slots[provider].release()
