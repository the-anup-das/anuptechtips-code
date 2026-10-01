"""Break it: a stall outlives a redis-py lock, and an update is lost.

A takes a 2 s lock, reads the counter, stalls for 3 s, then writes value + 1.
B starts 0.5 s later, gets the lock when A's lease expires, and writes value + 1.
Two increments; the counter ends at 1.        Run: python break_it.py
"""
import os
import pathlib
import threading
import time

import psycopg
import redis
from redis.exceptions import LockNotOwnedError

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
PG_DSN = os.environ.get("PG_DSN", "postgresql://patterns:patterns@localhost:55432/locks")
r = redis.Redis.from_url(REDIS_URL)


def worker(name: str, stall: float) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        lock = r.lock("lock:counter", timeout=2, blocking_timeout=10)
        if not lock.acquire():
            return
        (value,) = db.execute("SELECT value FROM counters WHERE id = 1").fetchone()
        time.sleep(stall)  # A: 3 s. Nothing tells A that its lease ended at 2 s.
        db.execute("UPDATE counters SET value = %s WHERE id = 1", (value + 1,))
        try:
            lock.release()
        except LockNotOwnedError:
            print(f"{name}: the lock expired before release")


def run() -> int:
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        db.execute(pathlib.Path(__file__).with_name("schema.sql").read_text())
        db.execute("INSERT INTO counters (id) VALUES (1) "
                   "ON CONFLICT (id) DO UPDATE SET value = 0, fence = 0")
    a = threading.Thread(target=worker, args=("A", 3.0))
    b = threading.Thread(target=worker, args=("B", 0.05))
    a.start()
    time.sleep(0.5)
    b.start()
    a.join()
    b.join()
    with psycopg.connect(PG_DSN) as db:
        return db.execute("SELECT value FROM counters WHERE id = 1").fetchone()[0]


if __name__ == "__main__":
    print("final value:", run())  # 1, not 2
