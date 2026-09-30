"""Two copies of the tutorial relay vs two SKIP LOCKED workers, same 10,000 rows.

Each copy runs in its own thread with its own autocommit connection and a stub
publisher (about 2 ms per batch, batch 100). Counts how many events reached the
stub more than once. Five runs each.

    python measure_naive.py
"""
from __future__ import annotations

import collections
import json
import pathlib
import sys
import threading

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN, reset_schema  # noqa: E402
from harness import seed  # noqa: E402
from naive_relay import naive_relay_batch  # noqa: E402
from relay import StubPublisher, relay_batch  # noqa: E402

ROWS, BATCH, COPIES, RUNS = 10_000, 100, 2, 5


def one_run(batch_fn) -> dict:
    reset_schema()
    with psycopg.connect(DSN, autocommit=True) as conn:
        seed(conn, ROWS)
    pubs = [StubPublisher() for _ in range(COPIES)]
    start = threading.Barrier(COPIES)

    def worker(pub):
        with psycopg.connect(DSN, autocommit=True) as conn:
            start.wait()
            while batch_fn(conn, pub, BATCH):
                pass

    threads = [threading.Thread(target=worker, args=(p,)) for p in pubs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    sent = collections.Counter(i for p in pubs for i in p.sent)
    return {"events": ROWS, "unique_sent": len(sent), "sent_total": sum(sent.values()),
            "sent_twice_or_more": sum(1 for c in sent.values() if c > 1),
            "extra_copies": sum(c - 1 for c in sent.values())}


def main() -> None:
    results = {}
    with measuring("outbox-naive-vs-skip-locked"):
        for name, fn in (("naive", naive_relay_batch), ("skip_locked", relay_batch)):
            results[name] = [one_run(fn) for _ in range(RUNS)]
            print(name, results[name], flush=True)
    (HERE / "results" / "naive_two_relays.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
