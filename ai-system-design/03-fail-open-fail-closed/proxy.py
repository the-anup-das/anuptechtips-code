"""A stand-in proxy. It reads feature files from its cohort's Kafka topic and scores a steady
stream of synthetic requests with whatever its holder is serving. There is no model and no
real traffic: a request is a dict of feature values and the answer is an HTTP status."""
import collections
import json
import threading
import time

from confluent_kafka import Consumer, TopicPartition

from lab import COHORTS, KAFKA_CLIENT, health_key, redis_client, topic

BLOCK_BELOW = 30                                   # the customer's rule: block scores under 30
FEATURES = [f"feature_{i:02d}" for i in range(1, 61)]
HUMAN = dict.fromkeys(FEATURES, 0.9)               # scores 89 with a good file
BOT = dict.fromkeys(FEATURES, 0.1)                 # scores 11
FLUSH = 0.1                                        # seconds between health reports to Redis


def is_bot(i: int) -> bool:
    return i % 10 == 9                             # every tenth request comes from a bot


def handle(holder, request: dict[str, float], on_unknown: str = "open") -> int:
    """One request through the bot-scoring step. Returns the status the client gets."""
    try:
        score = holder.score(request)
    except Exception:
        return 500                                 # the crash: an unhandled error per request
    if score is None:                              # no usable file: decided in advance, per route
        return 200 if on_unknown == "open" else 503
    return 403 if score < BLOCK_BELOW else 200


class Proxy:
    def __init__(self, name: str, cohort: int, holder, run: str, offset: int,
                 on_unknown: str = "open", epoch: float = 0.0):
        self.name, self.cohort, self.holder, self.run = name, cohort, holder, run
        self.on_unknown, self.epoch = on_unknown, epoch
        self.statuses = collections.Counter()          # status -> requests
        self.humans_blocked = self.bots_passed = 0     # the two ways to score wrongly
        self.stale = 0                                 # served on an old file, a newer one rejected
        self.served_by_slot = collections.Counter()    # tenth of a second since epoch -> requests
        self.errors_by_slot = collections.Counter()    # ... -> 5xx answers
        self.first_error = self.last_error = None      # perf_counter() of the first and last 5xx
        self.sent = 0
        self._pending = collections.Counter()          # (version, field) -> count not yet in Redis
        self._redis = redis_client()
        self._consumer = Consumer({**KAFKA_CLIENT, "group.id": f"failmodes.{name}",
                                   "enable.auto.commit": False, "fetch.wait.max.ms": 10})
        self._consumer.assign([TopicPartition(topic(cohort), 0, offset)])

    def poll(self, timeout: float = 0.0) -> bool:
        """Take one feature file from the topic, if there is one, and hand it to the holder."""
        msg = self._consumer.poll(timeout)
        if msg is None or msg.error():
            return False
        doc = json.loads(msg.value())
        if doc["run"] != self.run:                     # a file for another fleet
            return False
        rejected = self.holder.rejected
        self.holder.apply(doc["version"], doc["features"])
        if self.holder.rejected > rejected:            # tell the rollout gate straight away
            self._redis.hincrby(health_key(self.run, self.cohort, doc["version"]), "rejected", 1)
        return True

    def serve(self, count: int = 1) -> None:
        for _ in range(count):
            bot = is_bot(self.sent)
            status = handle(self.holder, BOT if bot else HUMAN, self.on_unknown)
            self.sent += 1
            self.statuses[status] += 1
            self.humans_blocked += status == 403 and not bot
            self.bots_passed += status == 200 and bot
            self.stale += self.holder.seen > self.holder.version > 0
            now = time.perf_counter()
            slot = int((now - self.epoch) * 10)
            self.served_by_slot[slot] += 1
            if status >= 500:
                self.first_error = self.first_error or now
                self.last_error = now
                self.errors_by_slot[slot] += 1
            field = "err" if status >= 500 else "blocked" if status == 403 else "ok"
            self._pending[self.holder.version, field] += 1

    def flush(self) -> None:
        """Report the request counters to Redis, where the rollout gate reads them."""
        pipe = self._redis.pipeline(transaction=False)
        for (version, field), count in self._pending.items():
            key = health_key(self.run, self.cohort, version)
            pipe.hincrby(key, field, count)
            pipe.expire(key, 600)
        pipe.execute()
        self._pending.clear()

    def close(self) -> None:
        self.flush()
        self._consumer.close()
        self._redis.close()


class ProxyThread(threading.Thread):
    """Runs one Proxy in real time: `rate` requests a second, a health report every FLUSH."""

    def __init__(self, proxy: Proxy, rate: float = 100.0):
        super().__init__(daemon=True, name=proxy.name)
        self.proxy, self.interval = proxy, 1.0 / rate
        self.serving = threading.Event()               # set it to start the request stream
        self._halt = threading.Event()

    def run(self) -> None:
        next_request = next_flush = None
        while not self._halt.is_set():
            now = time.perf_counter()
            if not self.serving.is_set():
                self.proxy.poll(0.01)
                continue
            if next_request is None:
                next_request, next_flush = now, now + FLUSH
            self.proxy.poll(max(0.0, min(next_request - now, 0.01)))
            now = time.perf_counter()
            while next_request <= now:                 # catch up if a poll ran long
                self.proxy.serve()
                next_request += self.interval
            if now >= next_flush:
                self.proxy.flush()
                next_flush = now + FLUSH
        self.proxy.close()

    def stop(self) -> None:
        self._halt.set()
        self.join()


def start_fleet(holder_factory, run: str, offsets: dict[int, int], on_unknown: str = "open",
                epoch: float = 0.0) -> list[Proxy]:
    """Twelve proxies in cohorts of 1, 3 and 8, each with its own holder and consumer."""
    fleet = []
    for cohort, size in enumerate(COHORTS):
        for _ in range(size):
            fleet.append(Proxy(f"proxy-{len(fleet):02d}", cohort, holder_factory(), run,
                               offsets[cohort], on_unknown, epoch))
    return fleet
