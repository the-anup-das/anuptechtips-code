"""KafkaPublisher against the Kafka 4 broker from docker-compose.yml (topics outbox.test-*)."""
import json
import uuid

import pytest
from confluent_kafka import Consumer

from db import KAFKA
from harness import seed
from relay import KafkaPublisher, PublishError, relay_batch


def consume(topic: str, n: int, timeout: float = 20.0) -> list:
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"test-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got = []
    try:
        for _ in range(int(timeout / 0.5)):
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                got.append(msg)
            if len(got) >= n:
                break
    finally:
        c.close()
    return got


@pytest.fixture
def prefix():
    return f"outbox.test-{uuid.uuid4().hex[:8]}."


def test_relay_publishes_to_kafka_keyed_by_aggregateid(conn, prefix):
    seed(conn, 5)
    relay_batch(conn, KafkaPublisher(KAFKA, topic_prefix=prefix), batch_size=10)

    msgs = consume(prefix + "order", 5)
    rows = {str(r[0]): (r[1], r[2]) for r in conn.execute(
        "SELECT id, aggregateid, payload FROM outbox WHERE published_at IS NOT NULL")}
    assert len(msgs) == 5 and len(rows) == 5
    for m in msgs:
        headers = {k: v.decode() for k, v in m.headers()}
        aggregateid, payload = rows[headers["id"]]
        assert m.key().decode() == aggregateid
        assert json.loads(m.value()) == payload
        assert headers["type"] == "OrderPlaced"


def test_an_oversized_event_is_refused_and_the_rest_still_go_out(conn, prefix):
    seed(conn, 3)
    conn.execute("UPDATE outbox SET payload = jsonb_build_object('n', 2, 'blob', repeat('x', 2000000)) "
                 "WHERE payload->>'n' = '2'")
    relay_batch(conn, KafkaPublisher(KAFKA, topic_prefix=prefix), batch_size=10)

    rows = {r[0]: r[1:] for r in conn.execute(
        "SELECT payload->>'n', published_at IS NOT NULL, attempts, last_error FROM outbox")}
    assert rows["1"][:2] == (True, 0) and rows["3"][:2] == (True, 0)
    published, attempts, error = rows["2"]
    assert (published, attempts) == (False, 1)
    assert "MSG_SIZE_TOO_LARGE" in error


def test_an_unreachable_broker_raises_and_leaves_rows_untouched(conn, prefix):
    seed(conn, 3)
    with pytest.raises(PublishError):
        relay_batch(conn, KafkaPublisher("localhost:1", topic_prefix=prefix, timeout=2),
                    batch_size=10)
    assert conn.execute(
        "SELECT count(*) FROM outbox WHERE published_at IS NULL AND attempts = 0").fetchone()[0] == 3
