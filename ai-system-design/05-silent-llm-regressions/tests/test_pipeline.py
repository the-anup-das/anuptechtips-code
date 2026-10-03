"""Events -> slice counters, with no double counting: on Postgres alone, then through Kafka."""
import threading
import uuid

import psycopg
from confluent_kafka import Producer

import harness
import pipeline
from conftest import needs_kafka
from evals import SYSTEM_PROMPT
from fake_model import Model
from serving import Fleet, serve, traffic
from test_slices import events


def totals(conn) -> tuple[int, int, int]:
    return conn.execute("SELECT coalesce(sum(total), 0), coalesce(sum(passed), 0), "
                        "(SELECT count(*) FROM processed_events) FROM slice_stats").fetchone()


def test_record_counts_events_per_slice(conn):
    pipeline.record(conn, events(10, passed=7) + events(5, passed=5, pool="long-context"))
    rows = conn.execute("SELECT pool, total, passed, script_ok FROM slice_stats ORDER BY pool")
    assert rows.fetchall() == [("long-context", 5, 5, 5), ("standard", 10, 7, 10)]


def test_redelivered_batch_is_not_counted_twice(conn):
    batch = events(50, passed=40)
    pipeline.record(conn, batch)
    pipeline.record(conn, batch)                      # the whole batch again
    pipeline.record(conn, batch[:10] + events(5, passed=5))   # a mix of old and new
    assert totals(conn) == (55, 45, 55)


def test_event_repeated_inside_one_batch_is_counted_once(conn):
    batch = events(3, passed=3)
    pipeline.record(conn, batch + batch + [batch[0]])
    assert totals(conn) == (3, 3, 3)


def test_two_consumers_recording_the_same_batch_at_once(dsn, conn):
    batch = events(400, passed=300)
    barrier = threading.Barrier(4)

    def worker() -> None:
        with psycopg.connect(dsn, autocommit=True) as c:
            barrier.wait()
            pipeline.record(c, batch)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert totals(conn) == (400, 300, 400)


@needs_kafka
def test_kafka_to_postgres_with_a_replayed_topic(dsn, conn):
    topic = f"regress.test-{uuid.uuid4().hex[:8]}.events"
    harness.create_topic(topic)
    produced = serve(Fleet(), Model(), SYSTEM_PROMPT, traffic(0, users=50), tick=0)
    producer = Producer({"bootstrap.servers": pipeline.BROKERS})
    pipeline.publish(producer, produced, topic)
    pipeline.publish(producer, produced[:60], topic)     # a producer retry: same IDs again
    del producer                                         # or it re-creates the topic we delete

    thread, stop = harness.start_consumer(topic, topic, dsn)
    try:
        harness.wait_for_events(dsn, len(produced))
    finally:
        stop.set()
        thread.join(15)
    assert totals(conn) == (200, sum(e["passed"] for e in produced), 200)

    # A second consumer group reads the topic from the start: every event is a duplicate.
    thread, stop = harness.start_consumer(topic, topic + ".again", dsn)
    try:
        harness.wait_caught_up(topic + ".again", topic)  # it has replayed all 260 messages
    finally:
        stop.set()
        thread.join(15)
    assert totals(conn) == (200, sum(e["passed"] for e in produced), 200)
    harness.delete_topic(topic)
