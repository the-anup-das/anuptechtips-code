"""Measurement 1: break it, then fence it (a correctness count, so no measure lock).

8 worker processes share one counter row and one FencedLock. Each critical section
takes the lock, reads the row, works, writes value + 1 and releases. In a fraction
of sections the worker also stalls for 1.5x the TTL before it writes. Rejected
writes are redone, so every mode ends with SECTIONS accepted increments, and

    lost updates = accepted writes - final counter value

Modes: none (plain write), write-only fencing, claim-first fencing (fencing.py).
Durations are the post's story scaled down 10x (TTL 0.2 s, stall 0.3 s, work 5 ms,
lock polling 10 ms), so 45 runs of 10,000 sections finish in well under an hour.

    python measure_fencing.py                      # the full grid -> results/fencing_runs.csv
    python measure_fencing.py --runs 1 --sections 1000 --out results/smoke.csv
    # Hochstein's overlap: long work, stalls of random length (the section always outlives the lease)
    python measure_fencing.py --work 0.1 --stall-min 0.1 --rates 0.05 --sections 2000 --out results/fencing_overlap.csv
"""
import argparse
import concurrent.futures as cf
import csv
import multiprocessing as mp
import os
import pathlib
import random
import time
import zlib

import psycopg
import redis
from redis.exceptions import LockNotOwnedError

from fencing import FencedLock, claim_then_write, fenced_write, unfenced_write

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:56379/5")
PG_DSN = os.environ.get("PG_DSN", "postgresql://patterns:patterns@localhost:55432/locks")
HERE = pathlib.Path(__file__).resolve().parent

TTL, STALL, POLL = 0.2, 0.3, 0.01
MODES = ("none", "write-only", "claim-first")
FIELDS = ["mode", "stall_rate", "run", "workers", "sections", "work_s", "ttl_s", "stall_min_s", "stall_s",
          "attempts", "accepted", "rejected", "stalls", "lock_not_owned", "stale_accepted",
          "final_value", "lost_updates", "elapsed_s"]


def worker(mode: str, rate: float, work: float, stall_min: float | None, seed: int, sections: int,
           row_id: int, lock_name: str, events_path: str, out: mp.Queue) -> None:
    r = redis.Redis.from_url(REDIS_URL)
    rng = random.Random(seed)
    stats = dict(attempts=0, accepted=0, rejected=0, stalls=0, lock_not_owned=0, stale_accepted=0)
    events = []
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        done = 0
        while done < sections:
            lock = FencedLock(r, lock_name, timeout=TTL, sleep=POLL, blocking_timeout=120)
            if not lock.acquire():
                raise RuntimeError("lock wait timed out")
            stalled = rng.random() < rate
            stall = STALL if stall_min is None else rng.uniform(stall_min, STALL)
            seen: dict[str, int] = {}

            def update(value: int) -> int:
                seen["read"] = value
                time.sleep(work)
                if stalled:
                    time.sleep(stall)  # with the work, longer than the TTL: the lease expires under us
                return value + 1

            t0 = time.perf_counter()
            if mode == "none":
                ok = unfenced_write(db, row_id, update)
            elif mode == "write-only":
                ok = fenced_write(db, row_id, lock.fence, update)
            else:
                ok = claim_then_write(db, row_id, lock.fence, update)
            t1 = time.perf_counter()
            try:
                lock.release()
                owned = True
            except LockNotOwnedError:
                owned = False
            stats["attempts"] += 1
            stats["stalls"] += stalled
            stats["accepted" if ok else "rejected"] += 1
            stats["lock_not_owned"] += not owned
            stats["stale_accepted"] += ok and not owned
            done += ok
            events.append((lock.fence, int(stalled), int(ok), int(owned), seen.get("read", -1), t0, t1))
    with open(events_path, "w", newline="") as f:
        csv.writer(f).writerows(events)
    out.put(stats)


def run_config(mode: str, rate: float, run: int, workers: int, sections: int, work: float,
               stall_min: float | None, events_dir: pathlib.Path) -> dict:
    tag = f"{mode}-{rate}-{run}-{work}-{stall_min}"
    row_id = 100 + zlib.crc32(tag.encode()) % 1_000_000
    lock_name = "{m1:" + tag + "}"
    r = redis.Redis.from_url(REDIS_URL)
    r.delete(lock_name, lock_name + ":fence")
    with psycopg.connect(PG_DSN, autocommit=True) as db:
        db.execute((HERE / "schema.sql").read_text())
        db.execute("INSERT INTO counters (id) VALUES (%s) "
                   "ON CONFLICT (id) DO UPDATE SET value = 0, fence = 0", (row_id,))
    ctx = mp.get_context("spawn")
    out = ctx.Queue()
    per = sections // workers
    procs = [ctx.Process(target=worker, args=(mode, rate, work, stall_min, run * 1000 + i, per, row_id,
                                              lock_name, str(events_dir / f"{tag}-w{i}.csv"), out))
             for i in range(workers)]
    started = time.perf_counter()
    for p in procs:
        p.start()
    results = [out.get() for _ in procs]
    for p in procs:
        p.join()
    elapsed = time.perf_counter() - started
    with psycopg.connect(PG_DSN) as db:
        final = db.execute("SELECT value FROM counters WHERE id = %s", (row_id,)).fetchone()[0]
    totals = {k: sum(res[k] for res in results) for k in results[0]}
    r.delete(lock_name, lock_name + ":fence")
    return dict(mode=mode, stall_rate=rate, run=run, workers=workers, sections=per * workers,
                work_s=work, ttl_s=TTL, stall_min_s=stall_min if stall_min is not None else STALL,
                stall_s=STALL, **totals, final_value=final,
                lost_updates=totals["accepted"] - final, elapsed_s=round(elapsed, 1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--sections", type=int, default=10_000)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--work", type=float, default=0.005)
    ap.add_argument("--rates", default="0.001,0.01,0.05")
    ap.add_argument("--stall-min", type=float, default=None, help="random stalls in [min, 0.3] s")
    ap.add_argument("--out", default="results/fencing_runs.csv")
    ap.add_argument("--events", default=os.environ.get("M1_EVENTS", str(HERE / "results" / "events")))
    # resume an interrupted grid: e.g. --first-run 5 --runs 1 --rates 0.05 --append
    ap.add_argument("--first-run", type=int, default=1, help="run number (and seed) to start at")
    ap.add_argument("--append", action="store_true", help="add rows to --out instead of starting it afresh")
    a = ap.parse_args()
    events_dir = pathlib.Path(a.events)
    events_dir.mkdir(parents=True, exist_ok=True)
    out_path = HERE / a.out
    if not (a.append and out_path.exists()):
        with open(out_path, "w", newline="") as f:
            csv.DictWriter(f, FIELDS).writeheader()
    for run in range(a.first_run, a.first_run + a.runs):
        for rate in [float(x) for x in a.rates.split(",")]:
            # the three modes run side by side, so they see the same machine load
            with cf.ThreadPoolExecutor(len(MODES)) as pool:
                rows = list(pool.map(lambda m: run_config(m, rate, run, a.workers, a.sections,
                                                          a.work, a.stall_min, events_dir), MODES))
            with open(out_path, "a", newline="") as f:
                csv.DictWriter(f, FIELDS).writerows(rows)
            for row in rows:
                print(f"run {run} rate {rate:<6} {row['mode']:<11} stalls {row['stalls']:>4} "
                      f"rejected {row['rejected']:>4} lost {row['lost_updates']:>5} "
                      f"({row['elapsed_s']} s)", flush=True)


if __name__ == "__main__":
    main()
