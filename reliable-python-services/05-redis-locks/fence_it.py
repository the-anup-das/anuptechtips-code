"""Fence it: break_it.py again, with a fencing token that Postgres checks.

    python fence_it.py pause write-only     A stalls past its lease; its write is rejected, A redoes -> 2
    python fence_it.py overlap write-only   Hochstein's overlap: stale A writes inside B's read-write gap -> 1
    python fence_it.py overlap claim-first  B claims the row before it reads, so A is rejected -> 2

Tokens start at 33, as in the post's diagrams (the row's fence starts at 32).
"""
import os
import pathlib
import sys
import threading
import time

import psycopg
import redis
from redis.exceptions import LockNotOwnedError

from fencing import FencedLock, claim_then_write, fenced_write

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
PG_DSN = os.environ.get("PG_DSN", "postgresql://patterns:patterns@localhost:55432/locks")
LOCK = "{lock:counter}"
WRITES = {"write-only": fenced_write, "claim-first": claim_then_write}


def worker(r: redis.Redis, name: str, stall: float, mode: str, log: list[str]) -> None:
    write = WRITES[mode]
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        while True:  # redo until our write is accepted
            lock = FencedLock(r, LOCK, timeout=2, blocking_timeout=10)
            if not lock.acquire():
                raise RuntimeError(f"{name}: lock busy")

            def work(value: int) -> int:
                time.sleep(stall)
                return value + 1

            accepted = write(db, 1, lock.fence, work)
            log.append(f"{name}: token {lock.fence} {'accepted' if accepted else 'REJECTED'}")
            try:
                lock.release()
            except LockNotOwnedError:
                log.append(f"{name}: token {lock.fence} had expired before release")
            if accepted:
                return
            stall = 0.05  # the redo is a normal, quick run


def run(scenario: str, mode: str) -> tuple[int, list[str]]:
    r = redis.Redis.from_url(REDIS_URL)
    r.set(f"{LOCK}:fence", 32)
    r.delete(LOCK)
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        db.execute(pathlib.Path(__file__).with_name("schema.sql").read_text())
        db.execute("INSERT INTO counters (id, value, fence) VALUES (1, 0, 32) "
                   "ON CONFLICT (id) DO UPDATE SET value = 0, fence = 32")
    b_stall = 1.5 if scenario == "overlap" else 0.05  # overlap: B is still working when A wakes
    log: list[str] = []
    a = threading.Thread(target=worker, args=(r, "A", 3.0, mode, log))
    b = threading.Thread(target=worker, args=(r, "B", b_stall, mode, log))
    a.start()
    time.sleep(0.5)
    b.start()
    a.join()
    b.join()
    with psycopg.connect(PG_DSN) as db:
        final = db.execute("SELECT value FROM counters WHERE id = 1").fetchone()[0]
    return final, log


if __name__ == "__main__":
    final, log = run(sys.argv[1], sys.argv[2])
    print("\n".join(log))
    print("final value:", final)
