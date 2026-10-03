"""Progressive rollout for config: publish a feature file one stage at a time, watch the
health of the proxies that got it, and put the last good file back if the gate says no."""
import json
import time
from collections.abc import Callable

import redis
from confluent_kafka import Producer

from lab import KAFKA_CLIENT, health_key, topic

STAGED = [(0,), (1,), (2,)]   # cohort by cohort: 1 proxy, then 3, then the other 8
GLOBAL = [(0, 1, 2)]          # everyone at once, which is what a "fast path" does


def health(r: redis.Redis, run: str, cohort: int, version: int, min_requests: int = 20,
           max_errors: float = 0.01, max_blocked: float = 0.5) -> bool | None:
    """True or False once the cohort has served enough requests on this version to tell.
    None means "too early to say", which the rollout must not read as good news."""
    seen = {k.decode(): int(v) for k, v in r.hgetall(health_key(run, cohort, version)).items()}
    if seen.get("rejected"):                      # a proxy refused the file outright
        return False
    ok, err, blocked = (seen.get(field, 0) for field in ("ok", "err", "blocked"))
    total = ok + err + blocked
    if total < min_requests:
        return None
    return err / total <= max_errors and blocked / total <= max_blocked


def rollout(version: int, names: list[str], last_good: list[str] | None,
            publish: Callable[[int, int, list[str]], None],
            gate: Callable[[int, int], bool | None],
            stages: list[tuple[int, ...]] = STAGED, soak: float = 1.0, poll: float = 0.05) -> int:
    """Publish one feature file stage by stage. `gate(cohort, version)` is health() bound to
    a Redis client and a run. Returns how many stages passed the gate."""
    reached: list[int] = []
    for passed, stage in enumerate(stages):
        for cohort in stage:
            publish(cohort, version, names)
        reached += stage
        deadline = time.monotonic() + soak
        verdict = None
        while verdict is not False and time.monotonic() < deadline:
            time.sleep(poll)
            verdicts = [gate(cohort, version) for cohort in stage]
            verdict = False if False in verdicts else None if None in verdicts else True
        if verdict is not True:                   # unhealthy, or no data by the deadline
            if last_good is not None:             # the rollback is one more version
                for cohort in reached:
                    publish(cohort, version + 1, last_good)
            return passed
    return len(stages)


def kafka_publisher(run: str) -> Callable[[int, int, list[str]], None]:
    """publish(cohort, version, names): one JSON message on the cohort's topic. The run id
    lets several fleets share the topics without loading each other's files."""
    producer = Producer({**KAFKA_CLIENT, "linger.ms": 0, "acks": "all"})

    def publish(cohort: int, version: int, names: list[str]) -> None:
        message = {"run": run, "version": version, "features": names}
        producer.produce(topic(cohort), json.dumps(message))
        producer.flush()

    return publish
