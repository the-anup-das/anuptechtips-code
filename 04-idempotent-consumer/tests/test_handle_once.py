"""The inbox logic against Postgres only, with a simulated at-least-once queue.

The queue keeps a committed offset. A consumer that crashes starts again from that
offset, which is exactly how Kafka redelivers after a crash.
"""
import threading
import time

import psycopg
import pytest
from confluent_kafka import Message

import consume
from consume import credit_account, event_id_of, handle_once
from worker import plain_once


class Crash(Exception):
    """Stands in for the process dying at a chosen point."""


class AtLeastOnceQueue:
    def __init__(self, events: list[tuple[str, dict]]):
        self.events = events
        self.committed = 0  # next offset to deliver after a restart

    def deliveries(self):
        return list(enumerate(self.events))[self.committed:]

    def commit(self, offset: int) -> None:
        self.committed = offset + 1


def consume_commit_last(conn, queue, handle=handle_once, crash_after_db_commit_at=None):
    """The post's order: database transaction first, offset second."""
    for offset, (event_id, event) in queue.deliveries():
        handle(conn, event_id, event)
        if offset == crash_after_db_commit_at:
            raise Crash
        queue.commit(offset)


def ledger_counts(conn) -> dict[str, int]:
    return dict(conn.execute("SELECT event_id, count(*) FROM ledger GROUP BY event_id").fetchall())


def balance(conn, account_id: int) -> int:
    return conn.execute("SELECT balance FROM accounts WHERE id = %s", (account_id,)).fetchone()[0]


EVENTS = [(f"evt-{i}", {"account_id": 1, "amount": 100}) for i in range(3)]


def test_double_delivery_is_applied_once(conn):
    event_id, event = EVENTS[0]
    assert handle_once(conn, event_id, event) is True
    assert handle_once(conn, event_id, event) is False
    assert ledger_counts(conn) == {"evt-0": 1}
    assert balance(conn, 1) == 100


def test_crash_between_db_commit_and_offset_commit(conn):
    queue = AtLeastOnceQueue(EVENTS)
    with pytest.raises(Crash):
        consume_commit_last(conn, queue, crash_after_db_commit_at=1)
    assert queue.committed == 1  # evt-1 is in the database but its offset isn't committed
    consume_commit_last(conn, queue)  # the restart redelivers evt-1
    assert ledger_counts(conn) == {"evt-0": 1, "evt-1": 1, "evt-2": 1}
    assert balance(conn, 1) == 300
    assert queue.committed == 3


def test_same_crash_without_the_inbox_applies_twice(conn):
    queue = AtLeastOnceQueue(EVENTS)
    with pytest.raises(Crash):
        consume_commit_last(conn, queue, handle=plain_once, crash_after_db_commit_at=1)
    consume_commit_last(conn, queue, handle=plain_once)
    assert ledger_counts(conn) == {"evt-0": 1, "evt-1": 2, "evt-2": 1}
    assert balance(conn, 1) == 400  # one customer credited twice


def test_offset_first_is_at_most_once(conn):
    queue = AtLeastOnceQueue(EVENTS)

    def failing_apply(c, event_id, event):
        if event_id == "evt-1":
            raise Crash
        credit_account(c, event_id, event)

    with pytest.raises(Crash):
        for offset, (event_id, event) in queue.deliveries():
            queue.commit(offset)  # the wrong order: offset first
            handle_once(conn, event_id, event, apply=failing_apply)
    consume_commit_last(conn, queue)
    assert ledger_counts(conn) == {"evt-0": 1, "evt-2": 1}  # evt-1 is gone for good


def test_exception_rolls_back_the_inbox_row_too(conn):
    event_id, event = EVENTS[0]

    def broken(c, eid, ev):
        credit_account(c, eid, ev)
        raise RuntimeError("downstream blew up after the first write")

    with pytest.raises(RuntimeError):
        handle_once(conn, event_id, event, apply=broken)
    assert conn.execute("SELECT count(*) FROM processed_messages").fetchone()[0] == 0
    assert ledger_counts(conn) == {}
    assert balance(conn, 1) == 0
    assert handle_once(conn, event_id, event) is True  # the redelivery still runs
    assert ledger_counts(conn) == {"evt-0": 1}


def race(dsn, first_outcome: str):
    """Two consumers get the same message; the first holds its transaction open."""
    event_id, event = EVENTS[0]
    claimed = threading.Event()
    results = {}

    def slow_apply(c, eid, ev):
        claimed.set()
        time.sleep(0.5)
        if first_outcome == "fail":
            raise RuntimeError("zombie died mid-transaction")
        credit_account(c, eid, ev)

    def first():
        with psycopg.connect(dsn, autocommit=True) as c:
            try:
                results["first"] = handle_once(c, event_id, event, apply=slow_apply)
            except RuntimeError:
                results["first"] = "error"

    def second():
        claimed.wait(5)
        with psycopg.connect(dsn, autocommit=True) as c:
            t0 = time.perf_counter()
            results["second"] = handle_once(c, event_id, event)
            results["second_waited"] = time.perf_counter() - t0

    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    return results


def test_two_racing_consumers_primary_key_serializes_them(dsn, conn):
    r = race(dsn, first_outcome="commit")
    assert (r["first"], r["second"]) == (True, False)
    assert r["second_waited"] > 0.3  # the second INSERT waited on the first transaction
    assert ledger_counts(conn) == {"evt-0": 1}


def test_if_the_first_consumer_rolls_back_the_second_does_the_work(dsn, conn):
    r = race(dsn, first_outcome="fail")
    assert (r["first"], r["second"]) == ("error", True)
    assert ledger_counts(conn) == {"evt-0": 1}


def test_inbox_is_scoped_per_consumer(conn, monkeypatch):
    event_id, event = EVENTS[0]
    monkeypatch.setattr(consume, "CONSUMER_NAME", "wallet-credits")
    assert handle_once(conn, event_id, event) is True
    monkeypatch.setattr(consume, "CONSUMER_NAME", "loyalty-points")
    assert handle_once(conn, event_id, event) is True  # another consumer, its own row
    assert handle_once(conn, event_id, event) is False
    rows = conn.execute("SELECT consumer FROM processed_messages ORDER BY 1").fetchall()
    assert rows == [("loyalty-points",), ("wallet-credits",)]


def test_event_id_comes_from_the_header_not_the_offset():
    assert event_id_of(Message(topic="t", value=b"{}", headers=[("event_id", b"abc")])) == "abc"
    assert event_id_of(Message(topic="t", value=b"{}", headers=[("trace", b"x")])) is None
    assert event_id_of(Message(topic="t", value=b"{}")) is None
