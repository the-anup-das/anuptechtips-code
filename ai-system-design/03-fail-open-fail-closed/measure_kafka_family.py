"""Plumbing check: why lab.KAFKA_CLIENT pins the Kafka clients to IPv4.

"localhost" resolves to both ::1 and 127.0.0.1. This produces and flushes one feature file
(60 names, about 1 KB of JSON) at a time to the broker's published port, 100 times per round,
once with the client pinned to IPv4 and once pinned to IPv6. 5 rounds per family.

Writes results/m0_kafka_address_family.json.
"""
import json
import pathlib
import statistics
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

import lab
from proxy import FEATURES

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))                 # the series folder
from common.measure_lock import measuring  # noqa: E402

TOPIC = "failmodes.plumbing"
ROUNDS, MESSAGES = 5, 100
MESSAGE = json.dumps({"run": "plumbing", "version": 1, "features": FEATURES})


def publish_times(family: str) -> list[float] | None:
    """Milliseconds per produce + flush, or None if the broker can't be reached that way."""
    producer = Producer({"bootstrap.servers": lab.KAFKA, "broker.address.family": family,
                         "linger.ms": 0, "acks": "all"})
    producer.produce(TOPIC, MESSAGE)                 # connect and fetch metadata first
    if producer.flush(15) > 0:
        return None
    times = []
    for _ in range(MESSAGES):
        started = time.perf_counter()
        producer.produce(TOPIC, MESSAGE)
        producer.flush()
        times.append((time.perf_counter() - started) * 1000)
    return times


def main() -> None:
    admin = AdminClient(lab.KAFKA_CLIENT)
    if TOPIC not in admin.list_topics(timeout=10).topics:
        for future in admin.create_topics([NewTopic(TOPIC, 1, 1)]).values():
            future.result()
        time.sleep(1.0)                              # a new topic needs a moment to elect a leader

    samples: dict[str, list[float]] = {"v4": [], "v6": []}
    reachable = {"v4": True, "v6": True}
    with measuring("fail-open-kafka-family", poll=0.0005):
        for _ in range(ROUNDS):
            for family in samples:
                times = publish_times(family) if reachable[family] else None
                if times is None:
                    reachable[family] = False
                else:
                    samples[family] += times

    summary = {"message_bytes": len(MESSAGE), "rounds": ROUNDS, "messages_per_round": MESSAGES}
    for family, ms in samples.items():
        if not ms:
            summary[family] = {"reachable": False}
            continue
        ms.sort()
        summary[family] = {"reachable": True, "publishes": len(ms),
                           "median_ms": round(statistics.median(ms), 2),
                           "p95_ms": round(ms[int(len(ms) * 0.95) - 1], 2),
                           "min_ms": round(ms[0], 2), "max_ms": round(ms[-1], 2)}
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    (out / "m0_kafka_address_family.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
