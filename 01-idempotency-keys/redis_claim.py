"""Idempotency keys in Redis: SET NX claims the key, a Lua script stores the result
only if the claim is still ours. Faster than Postgres, but not atomic with the work."""
import json
import logging
import uuid
from collections.abc import Callable

import redis

from idempotency import Result, replay_or_reject

LEASE_SECONDS = 30        # how long an in-progress claim lives if we crash
KEEP_SECONDS = 24 * 3600  # how long a finished result is replayed

log = logging.getLogger(__name__)

# Replace our claim with the final record ('' = delete it), unless someone else
# has claimed the key since. Plain Lua, so it runs on Redis 6.2+, 7.x and Valkey.
SWAP_IF_OURS = """
local current = redis.call('GET', KEYS[1])
if current and current ~= ARGV[1] then return 0 end
if ARGV[2] == '' then
  redis.call('DEL', KEYS[1])
else
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
end
return 1
"""


def run_once_redis(r: redis.Redis, client_id: str, key: str, fp: str,
                   do_work: Callable[[], tuple[int, dict]]) -> Result:
    rkey = f"idem:{client_id}:{key}"
    claim = json.dumps({"status": "in_progress", "fp": fp, "owner": uuid.uuid4().hex})

    # SET NX is the claim; GET hands back the existing record in the same round trip.
    existing = r.set(rkey, claim, nx=True, ex=LEASE_SECONDS, get=True)
    if existing is not None:
        rec = json.loads(existing)
        return replay_or_reject(fp, rec["fp"], rec["status"], rec.get("code"), rec.get("body"))

    swap = r.register_script(SWAP_IF_OURS)
    try:
        code, body = do_work()
    except Exception:
        swap(keys=[rkey], args=[claim, "", 0])  # the work failed cleanly: release the key
        raise

    record = json.dumps({"status": "completed", "fp": fp, "code": code, "body": body})
    if not swap(keys=[rkey], args=[claim, record, KEEP_SECONDS]):
        # The work outlived the lease and a retry claimed the key: it may run twice.
        log.warning("claim on %s expired mid-work; a retry may have repeated it", rkey)
    return Result(code, body)
