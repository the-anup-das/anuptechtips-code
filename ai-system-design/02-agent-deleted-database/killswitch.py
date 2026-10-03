"""A kill switch the agent can't reach. The gate sends every tool call to Kafka; this
consumer counts each agent's destructive calls and sets killswitch:<agent> in Redis when
they come too fast. Anything else that can set that key can stop the agent too.

    python killswitch.py stop <agent> [reason]    the manual switch
    python killswitch.py clear <agent>
    python killswitch.py watch                    run the watchdog until Ctrl+C
"""
import collections
import json
import sys
import threading
from collections.abc import Callable

import redis
from confluent_kafka import Consumer, Producer

import lab

TOPIC = lab.TOPIC_PREFIX + "tool-calls"
MAX_DESTRUCTIVE, WINDOW = 20, 10.0  # more than 20 destructive calls in 10 seconds trips it


def stop(r: redis.Redis, agent: str, reason: str = "stopped by hand") -> None:
    r.set(f"killswitch:{agent}", reason)


def clear(r: redis.Redis, agent: str) -> None:
    r.delete(f"killswitch:{agent}")


def audit_to_kafka(producer: Producer, topic: str = TOPIC) -> Callable[[dict], None]:
    """The gate's audit hook: one event per call, keyed by agent so each agent's stay in order."""
    def audit(event: dict) -> None:
        producer.produce(topic, key=event["agent"], value=json.dumps(event))
        producer.poll(0)
    return audit


def watch(r: redis.Redis, topic: str = TOPIC, group: str = "agentdel.watchdog",
          stopped: threading.Event | None = None, ready: threading.Event | None = None,
          max_destructive: int = MAX_DESTRUCTIVE, window: float = WINDOW) -> None:
    consumer = Consumer({"bootstrap.servers": lab.KAFKA, "group.id": group,
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([topic], on_assign=lambda c, partitions: ready and ready.set())
    recent: dict[str, collections.deque] = collections.defaultdict(collections.deque)
    try:
        while not (stopped and stopped.is_set()):
            msg = consumer.poll(0.05)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            if event["kind"] != "destructive":
                continue
            calls = recent[event["agent"]]  # attempts count, whether the gate allowed them or not
            calls.append(event["ts"])
            while calls[0] <= event["ts"] - window:
                calls.popleft()
            if len(calls) > max_destructive:
                # SET NX is idempotent: a redelivered event finds the switch already thrown.
                r.set(f"killswitch:{event['agent']}",
                      f"{len(calls)} destructive calls in {window:g} s", nx=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    client = redis.Redis.from_url(lab.REDIS_URL)
    command, *rest = sys.argv[1:] or ["watch"]
    if command == "stop":
        stop(client, rest[0], " ".join(rest[1:]) or "stopped by hand")
    elif command == "clear":
        clear(client, rest[0])
    else:
        watch(client)
