"""The version that races: check the budget, call the model, then add the cost.
Kept so the race test can show what it costs. Don't ship it."""
import redis.asyncio as redis

from budget import Reservation, limit_key, spent_key


class NaiveBudget:
    def __init__(self, r: redis.Redis) -> None:
        self.r = r

    async def reserve(self, org: str, levels: list[str], resv_id: str,
                      amount: int) -> Reservation | None:
        for level in levels:
            limit = await self.r.get(limit_key(org, level))
            spent = int(await self.r.get(spent_key(org, level)) or 0)
            if limit is not None and spent + amount > int(limit):
                return None
        # Nothing is written here. Every request that arrives before the first
        # one finishes reads the same `spent` and passes the same check.
        return Reservation("", tuple(spent_key(org, level) for level in levels), amount)

    async def settle(self, resv: Reservation, actual: int) -> int:
        for key in resv.spent_keys:
            await self.r.incrby(key, actual)  # the spend lands after the call
        return resv.amount - actual
