import threading
import time

import psycopg
import pytest

from conftest import claimable, pending
from harness import count_sent, finish, go, seed, spawn
from relay import CLAIM, MAX_ATTEMPTS, StubPublisher, relay_batch, run


class RefusingPublisher(StubPublisher):
    """Refuses the events whose payload n is in `bad`, like a broker rejecting a message."""

    def __init__(self, bad: set[int]):
        super().__init__(delay=0)
        self.bad = bad

    def publish(self, events):
        refused = {e.id: "MSG_SIZE_TOO_LARGE" for e in events if e.payload["n"] in self.bad}
        super().publish([e for e in events if e.id not in refused])
        return refused


class DownPublisher:
    def publish(self, events):
        raise ConnectionError("broker unreachable")


def test_relay_publishes_and_marks_every_row(conn):
    seed(conn, 250)
    pub = StubPublisher(delay=0)
    while relay_batch(conn, pub, batch_size=100):
        pass
    all_ids = [r[0] for r in conn.execute("SELECT id FROM outbox")]
    assert sorted(pub.sent) == sorted(all_ids)
    assert pending(conn) == 0


def test_two_workers_claim_disjoint_batches(dsn, conn):
    seed(conn, 30)
    with psycopg.connect(dsn) as a, psycopg.connect(dsn) as b:
        batch_a = {r[0] for r in a.execute(CLAIM, (20,))}  # a's transaction stays open
        batch_b = {r[0] for r in b.execute(CLAIM, (20,))}
        assert len(batch_a) == 20 and len(batch_b) == 10
        assert batch_a.isdisjoint(batch_b)
        a.rollback()
        b.rollback()


def test_refused_event_backs_off_and_the_rest_are_published(conn):
    seed(conn, 10)
    relay_batch(conn, RefusingPublisher(bad={3}), batch_size=10)
    rows = conn.execute(
        "SELECT payload->>'n', published_at IS NOT NULL, attempts, last_error, "
        "next_attempt_at > now(), dead_at FROM outbox ORDER BY (payload->>'n')::int").fetchall()
    for n, published, attempts, error, backed_off, dead_at in rows:
        if n == "3":
            assert (published, attempts, error, backed_off, dead_at) == (
                False, 1, "MSG_SIZE_TOO_LARGE", True, None)
        else:
            assert published and attempts == 0


def test_poison_row_goes_dead_and_stops_being_claimed(conn):
    seed(conn, 5)
    conn.execute("UPDATE outbox SET attempts = %s WHERE payload->>'n' = '1'", (MAX_ATTEMPTS - 1,))
    relay_batch(conn, RefusingPublisher(bad={1}), batch_size=10)
    dead = conn.execute("SELECT payload->>'n' FROM outbox WHERE dead_at IS NOT NULL").fetchall()
    assert dead == [("1",)]
    conn.execute("UPDATE outbox SET next_attempt_at = now() - interval '1 hour'")  # skip backoff
    assert claimable(conn) == 0  # the dead row is never claimed again


def test_unreachable_broker_rolls_back_the_whole_batch(conn):
    seed(conn, 10)
    with pytest.raises(ConnectionError):
        relay_batch(conn, DownPublisher(), batch_size=10)
    # nothing marked, no attempts burned: an outage isn't the rows' fault
    assert conn.execute(
        "SELECT count(*) FROM outbox WHERE published_at IS NULL AND attempts = 0").fetchone()[0] == 10
    assert claimable(conn) == 10


def test_idle_in_transaction_timeout_releases_a_hung_relay(dsn, conn):
    seed(conn, 5)
    with psycopg.connect(dsn, autocommit=True,
                         options="-c idle_in_transaction_session_timeout=1s") as hung:
        # Linux clients usually get FATAL 25P03 first; on Windows the socket abort wins.
        with pytest.raises((psycopg.errors.IdleInTransactionSessionTimeout,
                            psycopg.OperationalError)):
            with hung.transaction():
                claimed = hung.execute(CLAIM, (5,)).fetchall()
                assert len(claimed) == 5
                time.sleep(2)  # a publish that never returns
                hung.execute("UPDATE outbox SET published_at = now()")
    # Postgres ended the session, so its row locks are gone
    assert claimable(conn) == 5
    assert pending(conn) == 5


def test_token_bucket_caps_the_drain_rate(dsn, conn):
    seed(conn, 1000)
    done = threading.Event()
    pub = StubPublisher(delay=0)
    t = threading.Thread(target=run, args=(dsn, pub), daemon=True,
                         kwargs=dict(batch_size=50, poll_interval=0.01, max_per_second=500,
                                     stop=done.is_set))
    start = time.perf_counter()
    t.start()
    while len(pub.sent) < 1000:
        time.sleep(0.01)
    elapsed = time.perf_counter() - start
    done.set()
    t.join(5)
    # a full bucket covers the first 50; the other 950 take 950 / 500 = 1.9 s
    assert 1.7 < elapsed < 4, elapsed


def test_four_worker_processes_no_missing_no_duplicates(conn, tmp_path):
    seed(conn, 10_000)
    logs = [tmp_path / f"worker-{i}.log" for i in range(4)]
    procs = [spawn(batch=100, log=log) for log in logs]
    go(procs)
    rows = [finish(p)[0] for p in procs]

    sent = count_sent(logs)
    all_ids = {str(r[0]) for r in conn.execute("SELECT id FROM outbox")}
    assert sum(rows) == 10_000
    assert set(sent) == all_ids  # 0 missing
    assert max(sent.values()) == 1  # 0 duplicates
    assert min(rows) > 0  # every worker did some of the work
    assert pending(conn) == 0
