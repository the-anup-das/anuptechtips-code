"""Per-tenant budgets in Redis: reserve the worst case before the call, settle after it."""
import datetime
import logging
import pathlib
from dataclasses import dataclass

import redis.asyncio as redis

LUA = pathlib.Path(__file__).resolve().parent / "lua"
RESERVATION_TTL_S = 300  # longer than the slowest request the gateway allows

log = logging.getLogger("gateway.budget")


@dataclass(frozen=True)
class Reservation:
    key: str
    spent_keys: tuple[str, ...]  # one counter per level: org, team, tenant, key
    amount: int                  # micro-USD held


def period() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")


def spent_key(org: str, level: str) -> str:
    # {org} is a Redis Cluster hash tag: every key of one org lands in the same slot,
    # which a script that touches several keys needs.
    return f"budget:{{{org}}}:{period()}:spent:{level}"


def limit_key(org: str, level: str) -> str:
    return f"budget:{{{org}}}:limit:{level}"


class Budget:
    def __init__(self, r: redis.Redis) -> None:
        self.r = r
        self._reserve = r.register_script((LUA / "reserve.lua").read_text())
        self._settle = r.register_script((LUA / "settle.lua").read_text())

    async def reserve(self, org: str, levels: list[str], resv_id: str,
                      amount: int) -> Reservation | None:
        """Hold `amount` micro-USD at every level, or at none. None: a level is out of budget."""
        spent = [spent_key(org, level) for level in levels]
        keys = [f"budget:{{{org}}}:resv:{resv_id}"]
        for level, counter in zip(levels, spent):
            keys += [counter, limit_key(org, level)]
        ok, denied_at = await self._reserve(keys=keys, args=[amount, RESERVATION_TTL_S])
        if not ok:
            log.info("over budget at %s: %s wanted %d micro-USD", levels[denied_at - 1], org, amount)
            return None
        return Reservation(keys[0], tuple(spent), amount)

    async def settle(self, resv: Reservation, actual: int) -> int:
        """Swap the hold for the real cost. Returns the refund, or -1 when there was no
        reservation left to settle (settled already, or expired and still charged)."""
        return await self._settle(keys=[resv.key, *resv.spent_keys], args=[actual])

    async def set_limit(self, org: str, level: str, micro_usd: int) -> None:
        await self.r.set(limit_key(org, level), micro_usd)

    async def spent(self, org: str, level: str) -> int:
        return int(await self.r.get(spent_key(org, level)) or 0)


class NoBudget:
    """Budgets switched off: every reservation succeeds and nothing is counted."""

    async def reserve(self, org: str, levels: list[str], resv_id: str,
                      amount: int) -> Reservation | None:
        return Reservation("", (), amount)

    async def settle(self, resv: Reservation, actual: int) -> int:
        return 0
