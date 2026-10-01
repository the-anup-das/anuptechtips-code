"""The tutorial relay is fine alone and double-publishes with two copies running."""
import collections
import threading

import psycopg

from harness import seed
from naive_relay import naive_relay_batch
from relay import StubPublisher, relay_batch


def run_two(dsn, batch_fn, rows=2_000) -> collections.Counter:
    pubs = [StubPublisher(), StubPublisher()]
    start = threading.Barrier(2)

    def worker(pub):
        with psycopg.connect(dsn, autocommit=True) as conn:
            start.wait()
            while batch_fn(conn, pub, 100):
                pass

    threads = [threading.Thread(target=worker, args=(p,)) for p in pubs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return collections.Counter(i for p in pubs for i in p.sent)


def test_one_naive_relay_is_fine(conn):
    seed(conn, 500)
    pub = StubPublisher(delay=0)
    while naive_relay_batch(conn, pub):
        pass
    assert len(pub.sent) == len(set(pub.sent)) == 500


def test_two_naive_relays_publish_the_same_rows(dsn, conn):
    seed(conn, 2_000)
    sent = run_two(dsn, naive_relay_batch)
    assert len(sent) == 2_000  # nothing lost...
    assert sum(c - 1 for c in sent.values()) > 0  # ...but plenty sent twice


def test_two_skip_locked_relays_publish_each_row_once(dsn, conn):
    seed(conn, 2_000)
    sent = run_two(dsn, relay_batch)
    assert len(sent) == 2_000
    assert max(sent.values()) == 1
