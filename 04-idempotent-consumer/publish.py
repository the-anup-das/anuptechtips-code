"""The producer side: every event carries its own ID in a header.

The ID is created once, with the event (in the outbox row, say), so a resend of the
same event carries the same ID. That's what the consumer dedups on.
"""
import json
import os

from confluent_kafka import Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:59092")


def make_producer() -> Producer:
    # librdkafka (and so confluent-kafka) defaults enable.idempotence to false
    return Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True})


def publish(producer: Producer, topic: str, event_id: str, event: dict) -> None:
    producer.produce(topic, key=str(event["account_id"]), value=json.dumps(event),
                     headers={"event_id": event_id})


if __name__ == "__main__":  # python publish.py 1000  -> 1000 demo credits on consumer.deposits
    import sys
    import uuid

    producer = make_producer()
    for seq in range(int(sys.argv[1]) if len(sys.argv) > 1 else 100):
        publish(producer, "consumer.deposits", str(uuid.uuid4()),
                {"account_id": seq % 100 + 1, "amount": 100, "seq": seq})
    producer.flush(30)
