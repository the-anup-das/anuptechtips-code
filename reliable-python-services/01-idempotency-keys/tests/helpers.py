"""Small helpers shared by the tests."""
import os
import threading
from collections.abc import Callable

import psycopg

from app import charge

REDIS_URL = os.environ.get("IDEM_REDIS_URL", "redis://localhost:56379/1")


def charges(conn: psycopg.Connection, client_id: str) -> int:
    return conn.execute("SELECT count(*) FROM charges WHERE client_id = %s",
                        (client_id,)).fetchone()[0]


def charge_work(client_id: str, amount: int = 1000) -> Callable[[psycopg.Connection], tuple[int, dict]]:
    return lambda conn: charge(conn, client_id, amount, "usd")


def all_at_once(n: int, fn: Callable[[int, threading.Barrier], object]) -> list[object]:
    """Run fn(i, barrier) in n threads; fn waits on the barrier right before the call
    under test. Returns each thread's result, or the exception it raised."""
    barrier = threading.Barrier(n)
    results: list[object] = [None] * n

    def worker(i: int) -> None:
        try:
            results[i] = fn(i, barrier)
        except Exception as exc:  # collected, so the test can count them
            results[i] = exc

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results
