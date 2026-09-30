import threading
import time

import psycopg
import pytest

from conftest import wait_until
from db import DSN, reset_schema
from harness import seed
from relay import StubPublisher
from wakeup import run_with_notify

INSERT = ("INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
          "VALUES ('order', 'order-1', 'OrderPlaced', '{}') RETURNING id")


@pytest.fixture
def dsn():
    reset_schema(notify=True)
    return DSN


@pytest.fixture
def relay(dsn):
    """Start run_with_notify in a thread; yields a function taking fallback_poll."""
    stop = threading.Event()
    threads = []

    def start(fallback_poll: float) -> StubPublisher:
        pub = StubPublisher(delay=0)
        t = threading.Thread(target=run_with_notify, args=(dsn, pub), daemon=True,
                             kwargs=dict(fallback_poll=fallback_poll, stop=stop.is_set))
        t.start()
        threads.append(t)
        time.sleep(0.3)  # let it LISTEN and finish its first drain
        return pub

    yield start
    stop.set()
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("NOTIFY outbox")  # wake it from notifies() so it sees stop
    for t in threads:
        t.join(10)


def test_notify_arrives_on_commit_and_never_on_rollback(dsn):
    with psycopg.connect(dsn, autocommit=True) as listener, psycopg.connect(dsn) as writer:
        listener.execute("LISTEN outbox")
        writer.execute(INSERT)
        writer.rollback()
        assert list(listener.notifies(timeout=0.5)) == []
        writer.execute(INSERT)
        writer.execute(INSERT)
        writer.commit()
        notes = list(listener.notifies(timeout=0.5))
        assert len(notes) == 1  # two inserts, one transaction: folded into one NOTIFY


def test_a_commit_wakes_the_relay_long_before_its_fallback_poll(conn, relay):
    pub = relay(fallback_poll=30)
    start = time.perf_counter()
    event_id = conn.execute(INSERT).fetchone()[0]
    assert wait_until(lambda: pub.sent, timeout=5) == [event_id]
    assert time.perf_counter() - start < 1


def test_rows_committed_while_the_relay_was_down_go_out_at_startup(conn, relay):
    seed(conn, 250)  # nobody is listening: these NOTIFYs are simply gone
    pub = relay(fallback_poll=30)
    assert len(wait_until(lambda: len(pub.sent) == 250 and pub.sent, timeout=5)) == 250


def test_the_fallback_poll_catches_a_row_whose_notify_never_came(dsn, relay):
    pub = relay(fallback_poll=1.0)
    with psycopg.connect(dsn, autocommit=True) as quiet:
        quiet.execute("SET session_replication_role = replica")  # triggers off: no NOTIFY
        event_id = quiet.execute(INSERT).fetchone()[0]
    assert wait_until(lambda: pub.sent, timeout=3) == [event_id]


def test_the_relay_reconnects_after_losing_its_listen_connection(conn, relay):
    pub = relay(fallback_poll=30)
    killed = conn.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = 'outbox' AND query = 'LISTEN outbox'").fetchall()
    assert killed == [(True,)]
    time.sleep(0.2)
    event_id = conn.execute(INSERT).fetchone()[0]  # its NOTIFY may land on nobody
    assert wait_until(lambda: pub.sent, timeout=5) == [event_id]
