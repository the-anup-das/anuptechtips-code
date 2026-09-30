import pathlib
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from cleanup import drop_expired, ensure_partitions, partition_name

HERE = pathlib.Path(__file__).resolve().parent.parent
TODAY = datetime.now(timezone.utc).date()
OLD = TODAY - timedelta(days=30)

INSERT_AT = ("INSERT INTO outbox (aggregatetype, aggregateid, type, payload, created_at) "
             "VALUES ('order', %s, 'OrderPlaced', '{}', %s) RETURNING id")


def partitions(conn) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = 'outbox'::regclass")}


@pytest.fixture
def old_day(conn):
    ensure_partitions(conn, OLD, 1)
    return datetime(OLD.year, OLD.month, OLD.day, 12, tzinfo=timezone.utc)


def test_an_insert_with_no_partition_fails_the_whole_transaction(conn):
    far = datetime.now(timezone.utc) + timedelta(days=400)
    with pytest.raises(psycopg.errors.CheckViolation, match='no partition of relation "outbox"'):
        conn.execute(INSERT_AT, ("order-1", far))


def test_refuses_to_drop_a_partition_with_unpublished_rows(conn, old_day):
    conn.execute(INSERT_AT, ("order-1", old_day))
    late = conn.execute(INSERT_AT, ("order-2", old_day)).fetchone()[0]
    conn.execute("UPDATE outbox SET published_at = now() WHERE id <> %s", (late,))

    assert drop_expired(conn, TODAY, keep_days=7) == {partition_name(OLD): 1}
    assert partition_name(OLD) in partitions(conn)

    conn.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (late,))
    assert drop_expired(conn, TODAY, keep_days=7) == {partition_name(OLD): 0}
    assert partition_name(OLD) not in partitions(conn)


def test_dead_rows_are_copied_out_before_the_drop(conn, old_day):
    conn.execute(INSERT_AT, ("order-1", old_day))
    dead = conn.execute(INSERT_AT, ("order-2", old_day)).fetchone()[0]
    conn.execute("UPDATE outbox SET published_at = now() WHERE id <> %s", (dead,))
    conn.execute("UPDATE outbox SET dead_at = now(), attempts = 10, last_error = 'boom' "
                 "WHERE id = %s", (dead,))

    assert drop_expired(conn, TODAY, keep_days=7) == {partition_name(OLD): 0}
    assert conn.execute("SELECT id, last_error FROM outbox_dead").fetchall() == [(dead, "boom")]


def test_recent_partitions_are_left_alone(conn):
    before = partitions(conn)
    assert drop_expired(conn, TODAY, keep_days=7) == {}
    assert partitions(conn) == before


def test_monitoring_reports_the_age_of_the_oldest_pending_row(conn):
    ten_min_ago = datetime.now(timezone.utc) - timedelta(minutes=10)
    conn.execute(INSERT_AT, ("order-1", ten_min_ago))
    conn.execute(INSERT_AT, ("order-2", datetime.now(timezone.utc)))
    first = (HERE / "monitoring.sql").read_text().split(";")[0]
    oldest, count, retrying = conn.execute(first).fetchone()
    assert 590 < oldest < 700 and count == 2 and retrying == 0


def test_drop_gives_up_instead_of_queueing_behind_a_reader(dsn, conn, old_day):
    conn.execute(INSERT_AT, ("order-1", old_day))
    conn.execute("UPDATE outbox SET published_at = now()")
    with psycopg.connect(dsn) as reader:
        reader.execute("SELECT count(*) FROM outbox").fetchone()  # transaction left open
        with pytest.raises(psycopg.errors.LockNotAvailable, match="lock timeout"):
            drop_expired(conn, TODAY, keep_days=7)  # DROP needs the parent's ACCESS EXCLUSIVE lock
        reader.rollback()
    assert partition_name(OLD) in partitions(conn)
    assert drop_expired(conn, TODAY, keep_days=7) == {partition_name(OLD): 0}  # fine once it's gone
