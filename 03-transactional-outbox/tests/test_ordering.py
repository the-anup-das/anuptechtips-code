"""The commit-order gap, reproduced with two real transactions.

T1 inserts first, so its row gets the smaller id. T2 inserts second and
commits first. A reader that remembers "the last id I saw" moves past T1's
row before T1 commits, and never sees it.
"""
import psycopg

from conftest import wait_until
from db import ADMIN_DSN
from relay import CLAIM, Event, StubPublisher, relay_batch
from watermark import NaiveWatermark, SnapshotWatermark

INSERT = ("INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
          "VALUES ('order', %s, %s, '{}') RETURNING id")


def out_of_order_commits(dsn):
    """Open T1, then T2; T2 commits first. Returns (t1, id1, id2) with T1 still open."""
    t1 = psycopg.connect(dsn)
    t2 = psycopg.connect(dsn)
    id1 = t1.execute(INSERT, ("order-1", "OrderPlaced")).fetchone()[0]
    id2 = t2.execute(INSERT, ("order-2", "OrderPlaced")).fetchone()[0]
    assert id1 < id2  # uuidv7: the insert order
    t2.commit()
    t2.close()
    return t1, id1, id2


def test_naive_watermark_loses_the_row_that_commits_late(dsn, conn):
    reader = NaiveWatermark()
    t1, id1, id2 = out_of_order_commits(dsn)

    assert reader.poll(conn) == [id2]  # the watermark jumps past id1
    t1.commit()
    t1.close()
    assert reader.poll(conn) == []  # id1 is committed now, and never read
    assert conn.execute("SELECT count(*) FROM outbox WHERE id = %s", (id1,)).fetchone()[0] == 1


def test_snapshot_watermark_waits_for_t1_and_loses_nothing(dsn, conn):
    reader = SnapshotWatermark()
    t1, id1, id2 = out_of_order_commits(dsn)

    assert reader.poll(conn) == []  # T1 is still open, so nothing after it is safe yet
    t1.commit()
    t1.close()
    # Write transactions anywhere in the cluster (even in other databases) hold the
    # snapshot back for a moment, so the two rows may arrive over several polls.
    seen = []
    wait_until(lambda: seen.extend(reader.poll(conn)) or len(seen) >= 2)
    assert seen == [id1, id2]
    assert reader.poll(conn) == []


def test_skip_locked_relay_publishes_late_rows_out_of_order_but_keeps_them(dsn, conn):
    pub = StubPublisher(delay=0)
    t1, id1, id2 = out_of_order_commits(dsn)
    relay_batch(conn, pub)
    t1.commit()
    t1.close()
    relay_batch(conn, pub)
    assert pub.sent == [id2, id1]


def test_a_long_transaction_in_another_database_delays_the_snapshot_reader(dsn, conn):
    reader = SnapshotWatermark()
    other = psycopg.connect(ADMIN_DSN)  # the `patterns` database, not `outbox`
    other.execute("CREATE TEMP TABLE busy (x int)")
    other.execute("INSERT INTO busy VALUES (1)")  # now it holds a transaction id
    new_id = conn.execute(INSERT, ("order-9", "OrderPlaced")).fetchone()[0]  # autocommit

    assert reader.poll(conn) == []  # committed, but newer than the open transaction
    other.rollback()
    other.close()
    assert wait_until(lambda: reader.poll(conn)) == [new_id]


class TypeRecorder(StubPublisher):
    def __init__(self, broker: list[str]):
        super().__init__(delay=0)
        self.broker = broker

    def publish(self, events):
        self.broker.extend(e.type for e in events)
        return {}


def test_two_workers_can_send_paid_before_created(dsn, conn):
    """Per-aggregate order isn't free: worker A holds 'created' while B sends 'paid'."""
    conn.execute(INSERT, ("order-1", "OrderCreated"))
    conn.execute(INSERT, ("order-1", "OrderPaid"))
    broker: list[str] = []
    with psycopg.connect(dsn) as a:  # worker A: claims the oldest row, then stalls
        batch_a = [Event(*r) for r in a.execute(CLAIM, (1,))]
        assert [e.type for e in batch_a] == ["OrderCreated"]
        relay_batch(conn, TypeRecorder(broker), batch_size=10)  # worker B skips A's row
        broker.extend(e.type for e in batch_a)  # A finally publishes
        a.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (batch_a[0].id,))
    assert broker == ["OrderPaid", "OrderCreated"]
