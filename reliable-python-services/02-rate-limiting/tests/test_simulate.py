"""Properties the post claims for each algorithm, checked on the simulator."""
import random
from fractions import Fraction

import simulate as s


def random_traffic(seed: int, n: int = 3000, minutes: int = 5) -> list[int]:
    rng = random.Random(seed)
    return sorted(rng.randrange(0, minutes * s.WINDOW_MS) for _ in range(n))


def test_fixed_window_doubles_at_the_edge():
    out = s.run(s.FixedWindow(), s.edge_burst())
    assert s.max_in_span(out, 1000) == 200


def test_sliding_log_never_passes_the_limit():
    for seed in range(10):
        for arrivals in (random_traffic(seed), s.random_bursts(seed), s.edge_burst_then_steady()):
            out = s.run(s.SlidingLog(), arrivals)
            assert s.max_in_span(out, s.WINDOW_MS) <= 100


def test_sliding_counter_worked_example():
    counter = s.SlidingCounter()
    counter.counts = {0: 84, 1: 36}          # 84 last minute, 36 so far
    now = 60_000 + 15_000                    # 15 s into the current minute
    assert counter.estimate(now) == 99       # 84 * 0.75 + 36
    assert counter.allow(now) is True        # one more fits...
    assert counter.allow(now) is False       # ...then it's full


def test_sliding_counter_is_not_a_hard_cap():
    out = s.run(s.SlidingCounter(), s.edge_burst_then_steady())
    assert s.max_in_span(out, s.WINDOW_MS) > 150


def test_token_bucket_never_beats_b_plus_rt():
    for seed in range(10):
        out = s.run(s.TokenBucket(capacity=20), random_traffic(seed))
        for span_s in (1, 10, 60):
            bound = 20 + Fraction(100, 60) * span_s
            assert s.max_in_span(out, span_s * 1000) <= bound


def test_gcra_makes_the_token_buckets_decisions():
    for seed in range(20):
        arrivals = random_traffic(seed) + s.edge_burst()
        arrivals.sort()
        for burst in (1, 5, 20, 100):
            gcra, bucket = s.GCRA(burst=burst), s.TokenBucket(capacity=burst)
            assert [gcra.allow(t) for t in arrivals] == [bucket.allow(t) for t in arrivals]


def test_leaky_meter_makes_the_token_buckets_decisions():
    for seed in range(10):
        arrivals = random_traffic(seed)
        meter, bucket = s.LeakyBucketMeter(capacity=20), s.TokenBucket(capacity=20)
        assert [meter.allow(t) for t in arrivals] == [bucket.allow(t) for t in arrivals]


def test_leaky_queue_smooths_but_delays():
    queue = s.LeakyBucketQueue(capacity=20)
    offers = [(t, queue.offer(t)) for t in s.edge_burst()]
    releases = [r for _, r in offers if r is not None]
    delays = [r - t for t, r in offers if r is not None]
    assert max(delays) <= 12_000                          # 20 in the queue x 0.6 s
    assert all(b - a >= 600 for a, b in zip(releases, releases[1:]))
    assert s.max_in_span(releases, s.WINDOW_MS) <= 101


def test_max_in_span_is_half_open():
    assert s.max_in_span([0, 999, 1000], 1000) == 2


def test_m2_integer_counter_matches_the_simulator():
    import measure_counter_error as m2
    for seed in range(5):
        arrivals = random_traffic(seed) + s.edge_burst_then_steady()
        arrivals.sort()
        fast, exact = m2.Counter(), s.SlidingCounter()
        decisions = []
        for t in arrivals:
            ok = fast.fits(t)
            fast.curr += ok
            decisions.append(ok)
        assert decisions == [exact.allow(t) for t in arrivals]
