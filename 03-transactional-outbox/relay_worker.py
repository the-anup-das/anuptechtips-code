"""One relay worker process, for the measurements and the kill -9 test.

    python relay_worker.py --batch 100 --publisher stub --log sent-1.txt --exit-when-empty

Prints READY once connected. With --wait-for-go it then waits for a line on
stdin, so several workers start together. With --exit-when-empty it stops at
the first claim that finds nothing and prints DONE <rows> <first> <last>
(perf_counter at the first claim and at the last commit that published rows).
"""
from __future__ import annotations

import argparse
import sys
import time

import psycopg

from db import DSN, KAFKA
from relay import SESSION_OPTIONS, KafkaPublisher, StubPublisher, relay_batch


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=DSN)
    p.add_argument("--batch", type=int, default=100)
    p.add_argument("--publisher", choices=["stub", "kafka"], default="stub")
    p.add_argument("--topic-prefix", default="outbox.event.")
    p.add_argument("--log", help="stub only: append sent ids to this file")
    p.add_argument("--wait-for-go", action="store_true")
    p.add_argument("--exit-when-empty", action="store_true")
    a = p.parse_args()

    if a.publisher == "stub":
        publisher = StubPublisher(log=open(a.log, "a", encoding="ascii") if a.log else None)
    else:
        publisher = KafkaPublisher(KAFKA, a.topic_prefix)
        publisher.producer.list_topics(timeout=10)  # connect before the clock starts

    with psycopg.connect(a.dsn, autocommit=True, options=SESSION_OPTIONS) as conn:
        print("READY", flush=True)
        if a.wait_for_go:
            sys.stdin.readline()
        rows, first, last = 0, time.perf_counter(), None
        while True:
            n = relay_batch(conn, publisher, a.batch)
            if n:
                rows, last = rows + n, time.perf_counter()
            elif a.exit_when_empty:
                break
            else:
                time.sleep(0.05)
        print(f"DONE {rows} {first:.6f} {(last or first):.6f}", flush=True)


if __name__ == "__main__":
    main()
