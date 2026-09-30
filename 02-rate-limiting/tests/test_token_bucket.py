import threading

import pytest

from token_bucket import TokenBucket


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_burst_then_wait():
    clock = FakeClock()
    bucket = TokenBucket(rate=100 / 60, capacity=20, clock=clock)
    assert [bucket.try_acquire() for _ in range(20)] == [0.0] * 20
    assert bucket.try_acquire() == pytest.approx(0.6)   # one token every 0.6 s


def test_waiting_the_returned_time_is_enough():
    clock = FakeClock()
    bucket = TokenBucket(rate=100 / 60, capacity=20, clock=clock)
    for _ in range(20):
        bucket.try_acquire()
    wait = bucket.try_acquire()
    clock.now += wait
    assert bucket.try_acquire() == 0.0


def test_refill_stops_at_capacity():
    clock = FakeClock()
    bucket = TokenBucket(rate=100 / 60, capacity=20, clock=clock)
    for _ in range(20):
        bucket.try_acquire()
    clock.now += 3600                     # an hour idle refills 20, not 6,000
    assert sum(bucket.try_acquire() == 0.0 for _ in range(100)) == 20


def test_cost_spends_several_tokens():
    clock = FakeClock()
    bucket = TokenBucket(rate=100 / 60, capacity=20, clock=clock)
    assert bucket.try_acquire(cost=15) == 0.0
    assert bucket.try_acquire(cost=10) == pytest.approx(5 * 0.6)
    with pytest.raises(ValueError):
        bucket.try_acquire(cost=21)


def test_fifty_threads_admit_exactly_the_burst():
    clock = FakeClock()                   # frozen: no refill during the race
    bucket = TokenBucket(rate=100 / 60, capacity=20, clock=clock)
    start = threading.Barrier(50)
    results = []

    def worker():
        start.wait()
        results.append(bucket.try_acquire())

    threads = [threading.Thread(target=worker) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(0.0) == 20


def test_four_replicas_are_four_buckets():
    clock = FakeClock()
    replicas = [TokenBucket(rate=100 / 60, capacity=20, clock=clock) for _ in range(4)]
    allowed = sum(replicas[i % 4].try_acquire() == 0.0 for i in range(200))
    assert allowed == 80                  # the load balancer spreads the burst
