"""Measurement 3: the wait that redis-py's polling adds (timing, so it runs under the measure lock).

A holder takes the lock, keeps it for a random 100-200 ms and deletes it. Waiters
block in redis-py's Lock.acquire() with sleep=S (the default is 0.1 s). We time
the gap from the delete to the first waiter's successful acquire: the lock was
free and nobody had noticed yet. We also count the SET attempts the waiters send.

Waiter setups: 1 waiter; 8 waiters that start polling together (a cron tick: redis-py
sleeps a fixed interval with no jitter, so they stay in step); 8 waiters that arrive
at random moments within one polling interval.

    python measure_polling.py   -> results/polling_trials.csv, results/polling_summary.csv
"""
import csv
import os
import pathlib
import random
import statistics
import sys
import threading
import time

import redis
from redis.lock import Lock

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.measure_lock import measuring  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
SLEEPS = (0.1, 0.01, 0.001)
WAITERS = (("1 waiter", 1, False), ("8 together", 8, False), ("8 staggered", 8, True))
RUNS, TRIALS = 5, 60
NAME = "m3:lock"


class CountingLock(Lock):
    """redis-py's Lock, counting how many SET NX attempts acquire() makes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = 0

    def do_acquire(self, token) -> bool:
        self.attempts += 1
        return super().do_acquire(token)


def trial(r: redis.Redis, sleep: float, waiters: int, stagger: bool,
          rng: random.Random) -> tuple[float, int, float]:
    r.set(NAME, "holder", px=10_000)
    hold = rng.uniform(0.1, 0.2)
    # losers give up a little after the release; a lone waiter always wins
    give_up = hold + (0.15 if waiters > 1 else 1.0)
    barrier = threading.Barrier(waiters + 1)
    results: list[tuple[bool, float, CountingLock]] = []
    offsets = [rng.uniform(0, sleep) if stagger else 0.0 for _ in range(waiters)]

    def wait_for_it(offset: float) -> None:
        # thread_local=False: the main thread releases the winner's lock after the trial
        lock = CountingLock(r, NAME, timeout=10, sleep=sleep, blocking_timeout=give_up,
                            thread_local=False)
        barrier.wait()
        time.sleep(offset)
        ok = lock.acquire()
        results.append((ok, time.perf_counter(), lock))

    threads = [threading.Thread(target=wait_for_it, args=(o,)) for o in offsets]
    for t in threads:
        t.start()
    barrier.wait()
    started = time.perf_counter()
    time.sleep(hold)
    r.delete(NAME)  # the holder releases
    released = time.perf_counter()
    for t in threads:
        t.join()
    winners = [res for res in results if res[0]]
    assert len(winners) == 1, "exactly one waiter should get the lock"
    _, acquired, lock = winners[0]
    lock.release()
    attempts = sum(res[2].attempts for res in results)
    return (acquired - released) * 1000, attempts, released - started


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL)
    rows = []
    with measuring("redis-locks-polling"):
        for run in range(1, RUNS + 1):
            for sleep in SLEEPS:
                for setup, waiters, stagger in WAITERS:
                    rng = random.Random(run * 100 + waiters + stagger)
                    for i in range(TRIALS):
                        delay_ms, attempts, waited = trial(r, sleep, waiters, stagger, rng)
                        rows.append(dict(run=run, sleep_s=sleep, setup=setup, trial=i,
                                         extra_wait_ms=round(delay_ms, 3), set_attempts=attempts,
                                         held_s=round(waited, 4)))
            print("run", run, "done", flush=True)
    with open(HERE / "results" / "polling_trials.csv", "w", newline="") as f:
        w = csv.DictWriter(f, rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    summary = []
    for sleep in SLEEPS:
        for setup, _, _ in WAITERS:
            sel = [x for x in rows if x["sleep_s"] == sleep and x["setup"] == setup]
            delays = sorted(x["extra_wait_ms"] for x in sel)
            run_medians = [statistics.median(x["extra_wait_ms"] for x in sel if x["run"] == k)
                           for k in range(1, RUNS + 1)]
            sets_per_s = sum(x["set_attempts"] for x in sel) / sum(x["held_s"] for x in sel)
            summary.append(dict(
                sleep_s=sleep, setup=setup, trials=len(sel),
                median_ms=round(statistics.median(delays), 2),
                p95_ms=round(delays[int(0.95 * (len(delays) - 1))], 2),
                min_ms=round(delays[0], 2), max_ms=round(delays[-1], 2),
                run_median_min_ms=round(min(run_medians), 2),
                run_median_max_ms=round(max(run_medians), 2),
                set_per_s_all_waiters=round(sets_per_s, 1)))
    with open(HERE / "results" / "polling_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, summary[0].keys())
        w.writeheader()
        w.writerows(summary)
    for s in summary:
        print(s)


if __name__ == "__main__":
    main()
