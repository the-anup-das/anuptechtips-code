"""The whole Redis lock pattern: SET NX PX to take it, compare-and-delete to give it back."""
import random
import secrets
import time

import redis

# For Redis older than 8.4 (and Valkey/managed services without DELEX): the same
# compare-and-delete as one atomic script. redis-py's own Lock uses this approach.
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def acquire(r: redis.Redis, name: str, ttl_ms: int, wait_s: float) -> str | None:
    """Return our token, or None if the lock was still busy after wait_s seconds."""
    token = secrets.token_hex(16)  # unique per holder, so we can only ever delete our own lock
    deadline = time.monotonic() + wait_s
    while True:
        if r.set(name, token, nx=True, px=ttl_ms):  # one atomic command: value + expiry
            return token
        if time.monotonic() >= deadline:
            return None
        time.sleep(random.uniform(0.05, 0.15))  # random, so waiters don't retry in lockstep


def release(r: redis.Redis, name: str, token: str, use_delex: bool = True) -> bool:
    """Delete the lock only if it still holds our token. False: it had already expired."""
    if use_delex:  # Redis 8.4+
        return r.execute_command("DELEX", name, "IFEQ", token) == 1
    return r.eval(RELEASE_LUA, 1, name, token) == 1
