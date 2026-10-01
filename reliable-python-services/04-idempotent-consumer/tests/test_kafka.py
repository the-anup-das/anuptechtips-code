"""consume.run() against the real Kafka broker from docker-compose (skipped without it)."""
import os
import subprocess
import sys
import time
import uuid

import psycopg
from confluent_kafka import Consumer

import harness
from conftest import ROOT, needs_kafka
from publish import BROKERS, make_producer, publish

pytestmark = needs_kafka


def ledger_counts(dsn) -> dict[str, int]:
    with psycopg.connect(dsn) as c:
        return dict(c.execute("SELECT event_id, count(*) FROM ledger GROUP BY event_id").fetchall())


def start(args: list[str], dsn: str, log_path) -> subprocess.Popen:
    env = {**os.environ, "CONSUMER_DSN": dsn, "PYTHONUNBUFFERED": "1"}
    return subprocess.Popen([sys.executable, *args], env=env, cwd=ROOT,
                            stdout=open(log_path, "a"), stderr=subprocess.STDOUT)


def wait_caught_up(group: str, topic: str, timeout: float = 60) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if harness.lag(group, topic) == 0:
            return
        time.sleep(0.5)
    raise AssertionError("consumer never caught up")


def read_all(topic: str, expect: int, timeout: float = 20) -> list:
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": "reader-" + uuid.uuid4().hex,
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got, t0 = [], time.time()
    while len(got) < expect and time.time() - t0 < timeout:
        m = c.poll(0.5)
        if m is not None and not m.error():
            got.append(m)
    c.close()
    return got


def test_republished_event_is_skipped_and_missing_id_goes_to_dlq(dsn, tmp_path):
    topic = f"consumer.test.{uuid.uuid4().hex[:8]}"
    harness.create_topic(topic, partitions=1)
    p = make_producer()
    publish(p, topic, "e1", {"account_id": 1, "amount": 100, "seq": 0})
    publish(p, topic, "e2", {"account_id": 2, "amount": 100, "seq": 1})
    publish(p, topic, "e1", {"account_id": 1, "amount": 100, "seq": 0})  # a relay resend: new offset, same ID
    p.produce(topic, key="3", value=b'{"account_id": 3, "amount": 100}')  # no event_id header
    p.flush(10)

    log = tmp_path / "consume.log"
    proc = start(["-c", "import sys, logging, consume; logging.basicConfig(level=logging.INFO); "
                        "consume.run(sys.argv[1], sys.argv[2])", topic, topic], dsn, log)
    try:
        wait_caught_up(topic, topic)
    finally:
        proc.kill()
        proc.wait()
    assert ledger_counts(dsn) == {"e1": 1, "e2": 1}
    assert "skipped duplicate e1" in log.read_text()
    dlq = read_all(topic + ".dlq", expect=1)
    assert [m.value() for m in dlq] == [b'{"account_id": 3, "amount": 100}']
    harness.delete_topics([topic, topic + ".dlq"])


def crash_between_commits(variant: str, dsn, tmp_path) -> dict[str, int]:
    """Five events; the consumer dies after the DB commit of seq 2, then restarts."""
    topic = f"consumer.test.{uuid.uuid4().hex[:8]}"
    harness.create_topic(topic, partitions=1)
    p = make_producer()
    for seq in range(5):
        publish(p, topic, f"evt-{seq}", {"account_id": 1, "amount": 100, "seq": seq})
    p.flush(10)
    marker, log = tmp_path / "crashed", tmp_path / "crash.log"
    args = ["tests/crash_once.py", variant, topic, topic, "2", str(marker)]
    first = start(args, dsn, log)
    assert first.wait(60) == 1 and marker.exists()  # it died where we wanted it to
    second = start(args, dsn, log)
    try:
        wait_caught_up(topic, topic)
    finally:
        second.kill()
        second.wait()
    harness.delete_topics([topic])
    return ledger_counts(dsn)


def test_real_crash_between_commits_with_inbox_applies_once(dsn, tmp_path):
    assert crash_between_commits("c", dsn, tmp_path) == {f"evt-{i}": 1 for i in range(5)}
    assert "skipped duplicate evt-2" in (tmp_path / "crash.log").read_text()


def test_real_crash_between_commits_without_inbox_applies_twice(dsn, tmp_path):
    counts = crash_between_commits("b", dsn, tmp_path)
    assert counts == {"evt-0": 1, "evt-1": 1, "evt-2": 2, "evt-3": 1, "evt-4": 1}
