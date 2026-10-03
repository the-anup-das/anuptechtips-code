"""Model and prompt versions as one Redis hash: sticky cohorts, a soak timer, and a rollback
that is a single write."""
import hashlib
import os
import time

import redis

REDIS_URL = os.environ.get("REGRESS_REDIS_URL", "redis://localhost:56379/10")
KEY = "regress:rollout:assistant"

# Widen only if nobody has rolled back and the current step has soaked. One script, so a
# rollback can't slip in between the check and the write.
WIDEN = """
local halted, changed_at = unpack(redis.call('HMGET', KEYS[1], 'halted', 'changed_at'))
if halted == '1' or tonumber(ARGV[2]) - tonumber(changed_at) < tonumber(ARGV[3]) then
  return 0
end
redis.call('HSET', KEYS[1], 'percent', ARGV[1], 'changed_at', ARGV[2])
return 1
"""


def start(r: redis.Redis, stable: str, candidate: str, percent: int = 1,
          now: float | None = None) -> None:
    r.hset(KEY, mapping={"stable": stable, "candidate": candidate, "percent": percent,
                         "halted": 0, "changed_at": time.time() if now is None else now})


def cohort(user_id: str) -> int:
    """0-99, and the same on every request, so widening the rollout only ever adds users."""
    return int.from_bytes(hashlib.sha256(user_id.encode()).digest()[:4], "big") % 100


def version_for(r: redis.Redis, user_id: str) -> str:
    stable, candidate, percent = r.hmget(KEY, "stable", "candidate", "percent")
    return (candidate if cohort(user_id) < int(percent) else stable).decode()


def widen(r: redis.Redis, percent: int, soak_seconds: float, now: float | None = None) -> bool:
    """Raise the candidate's share. False if the soak isn't over or someone rolled back."""
    now = time.time() if now is None else now
    return bool(r.eval(WIDEN, 1, KEY, percent, now, soak_seconds))


def rollback(r: redis.Redis) -> None:
    r.hset(KEY, mapping={"percent": 0, "halted": 1})   # one HSET; the next request reads it
