"""Fencing tokens: the lock hands out a number that only grows, and Postgres
refuses any write that carries an older one.

Table: schema.sql (counters: id, value, fence).
"""
from collections.abc import Callable

import psycopg
import redis
from redis.lock import Lock

# Take the lock and issue the next token in one atomic step. A token fetched
# after acquiring could be handed out in the wrong order to a stale holder.
ACQUIRE_LUA = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
"""


class FencedLock(Lock):
    """redis-py's Lock (same blocking, release and extend), plus a fencing token.

    Name it with a hash tag, e.g. "{lock:job}", so the lock key and
    "{lock:job}:fence" land in the same Redis Cluster slot.
    """

    def __init__(self, r: redis.Redis, name: str, timeout: float, **kwargs):
        super().__init__(r, name, timeout=timeout, **kwargs)
        self.fence_key = f"{name}:fence"
        self._acquire = r.register_script(ACQUIRE_LUA)

    def do_acquire(self, token: bytes) -> bool:
        fence = self._acquire(keys=[self.name, self.fence_key],
                              args=[token, int(self.timeout * 1000)])
        self.local.fence = int(fence)
        return self.local.fence > 0

    @property
    def fence(self) -> int:
        return self.local.fence


Update = Callable[[int], int]  # old value -> new value; the slow part of the job


def unfenced_write(db: psycopg.Connection, row_id: int, update: Update) -> bool:
    """What break_it.py does: read, work, write. Always 'succeeds'."""
    (value,) = db.execute("SELECT value FROM counters WHERE id = %s", (row_id,)).fetchone()
    db.execute("UPDATE counters SET value = %s WHERE id = %s", (update(value), row_id))
    return True


def fenced_write(db: psycopg.Connection, row_id: int, fence: int, update: Update) -> bool:
    """Read, work, then write only if no newer token has written. False: we're stale."""
    (value,) = db.execute("SELECT value FROM counters WHERE id = %s", (row_id,)).fetchone()
    cur = db.execute(
        "UPDATE counters SET value = %s, fence = %s WHERE id = %s AND fence < %s",
        (update(value), fence, row_id, fence))
    return cur.rowcount == 1  # 0 rows: a newer holder wrote first. Abort and redo.


def claim_then_write(db: psycopg.Connection, row_id: int, fence: int, update: Update) -> bool:
    """Stamp our token on the row before reading it, then write only if it's still ours."""
    row = db.execute(
        "UPDATE counters SET fence = %s WHERE id = %s AND fence < %s RETURNING value",
        (fence, row_id, fence)).fetchone()
    if row is None:
        return False  # a newer holder already claimed the row
    cur = db.execute(
        "UPDATE counters SET value = %s WHERE id = %s AND fence = %s",
        (update(row[0]), row_id, fence))
    return cur.rowcount == 1  # 0 rows: a newer holder claimed it while we worked
