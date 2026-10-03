"""The audit trail on Kafka 4: relay, crash, duplicates and rebuild (topics chatbot.audit.test-*)."""
import pathlib
import subprocess
import sys

import pytest

from conftest import model
from rebuild import conversation, mismatches, read_topic, rebuild
from relay import make_producer, relay_batch
from service import handle

HERE = pathlib.Path(__file__).resolve().parent.parent
QUESTIONS = ["What is the deadline to apply for a refund?",
             "Can I apply for the bereavement fare after I have already flown?",
             "How much does a checked bag cost?",
             "Is it legal for an airline to overbook a flight?"]


def pending(conn) -> int:
    return conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]


def test_the_audit_event_reaches_kafka(conn, r, topic):
    out = handle(conn, r, model("faithful", reword_rate=0), "conv-1", QUESTIONS[0], topic=topic)

    assert relay_batch(conn, make_producer()) == 1
    assert pending(conn) == 0

    (event,) = read_topic(topic)
    assert event["conversation_id"] == "conv-1" and event["question"] == QUESTIONS[0]
    assert event["reply"] == out.reply and event["cited"] == [["REF-03", 1]]
    assert event["checks"] == {"problems": [], "tier2": False, "cached": False}
    assert mismatches(conn, rebuild([event])) == []


def test_a_conversation_can_be_rebuilt_in_order_from_kafka_alone(conn, r, topic):
    llm = model(seed=3)
    for i, question in enumerate(QUESTIONS * 3):
        handle(conn, r, llm, f"conv-{i % 2}", question, topic=topic)
    while relay_batch(conn, make_producer(), batch_size=5):
        pass

    trail = rebuild(read_topic(topic))
    assert len(trail) == 12 and mismatches(conn, trail) == []
    asked = [row[0] for row in conn.execute(
        "SELECT question FROM answer_audit WHERE conversation_id = 'conv-0' ORDER BY id")]
    assert [e["question"] for e in conversation(trail, "conv-0")] == asked


def test_a_relay_that_dies_after_the_ack_resends_the_batch_and_the_rebuild_still_matches(
        conn, r, topic):
    llm = model(seed=5)
    for i in range(30):
        handle(conn, r, llm, f"conv-{i % 3}", QUESTIONS[i % 4], topic=topic)

    # Batch 1 is marked. Batch 2 reaches Kafka, then the process dies before marking it.
    crashed = subprocess.run([sys.executable, "relay.py", "--batch", "10", "--until-empty",
                              "--crash-after", "2"], cwd=HERE, timeout=60)
    assert crashed.returncode == 1
    assert pending(conn) == 20                      # batch 2 was rolled back with the crash

    subprocess.run([sys.executable, "relay.py", "--batch", "10", "--until-empty"],
                   cwd=HERE, timeout=60, check=True)
    assert pending(conn) == 0

    events = read_topic(topic)
    trail = rebuild(events)
    assert len(events) == 40                        # 30 replies + the 10 that were sent twice
    assert len(trail) == 30 and mismatches(conn, trail) == []


def test_an_unreachable_broker_leaves_the_rows_pending(conn, r, topic):
    handle(conn, r, model(), "conv-1", QUESTIONS[0], topic=topic)
    from confluent_kafka import Producer
    nowhere = Producer({"bootstrap.servers": "localhost:1", "message.timeout.ms": 1500})
    with pytest.raises(RuntimeError):
        relay_batch(conn, nowhere)
    assert pending(conn) == 1


def test_a_missing_event_shows_up_as_a_mismatch(conn, r, topic):
    for i in range(3):
        handle(conn, r, model(), "conv-1", QUESTIONS[i], topic=topic)
    relay_batch(conn, make_producer())
    events = read_topic(topic)
    assert len(mismatches(conn, rebuild(events[:-1]))) == 1
