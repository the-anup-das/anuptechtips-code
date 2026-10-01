"""The six rate limiting algorithms as pure-Python simulations, one limit for all.

Time is an integer number of milliseconds and the maths is exact (integers or
fractions.Fraction), so two algorithms can be compared decision by decision
without floating-point noise. Every limiter has the same shape:

    limiter.allow(now_ms) -> bool

except the queueing leaky bucket, whose offer(now_ms) returns the time the
request leaves the queue (or None when the queue is full).

Run the measurements with measure_edge_burst.py and measure_counter_error.py.
"""
from __future__ import annotations

import bisect
import random
from collections import deque
from fractions import Fraction

LIMIT = 100          # requests...
WINDOW_MS = 60_000   # ...per minute


class FixedWindow:
    """One counter per calendar minute (12:00:00-12:00:59.999, and so on)."""

    def __init__(self, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.limit, self.window = limit, window_ms
        self.current, self.count = None, 0

    def allow(self, now: int) -> bool:
        window = now // self.window
        if window != self.current:
            self.current, self.count = window, 0
        if self.count < self.limit:
            self.count += 1
            return True
        return False


class SlidingLog:
    """Exact: keep a timestamp per accepted request from the last 60 s."""

    def __init__(self, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.limit, self.window = limit, window_ms
        self.log: deque[int] = deque()

    def allow(self, now: int) -> bool:
        while self.log and self.log[0] <= now - self.window:
            self.log.popleft()
        if len(self.log) < self.limit:
            self.log.append(now)
            return True
        return False


class SlidingCounter:
    """Two counters: estimate = previous * (1 - elapsed / window) + current."""

    def __init__(self, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.limit, self.window = limit, window_ms
        self.counts: dict[int, int] = {}

    def estimate(self, now: int) -> Fraction:
        window, elapsed = divmod(now, self.window)
        previous = self.counts.get(window - 1, 0)
        current = self.counts.get(window, 0)
        return previous * (1 - Fraction(elapsed, self.window)) + current

    def allow(self, now: int) -> bool:
        if self.estimate(now) + 1 <= self.limit:
            window = now // self.window
            self.counts[window] = self.counts.get(window, 0) + 1
            self.counts.pop(window - 2, None)
            return True
        return False


class TokenBucket:
    """Holds up to `capacity` tokens and refills at limit/window per ms."""

    def __init__(self, capacity: int = 20, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.capacity = capacity
        self.rate = Fraction(limit, window_ms)        # tokens per ms
        self.tokens = Fraction(capacity)              # start full
        self.last: int | None = None

    def allow(self, now: int) -> bool:
        if self.last is not None:
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class GCRA:
    """Generic cell rate algorithm: one timestamp, the theoretical arrival time.

    T is the emission interval (600 ms at 100/min) and tau = (burst - 1) * T is
    how far ahead of schedule a client may run.
    """

    def __init__(self, burst: int = 20, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.T = Fraction(window_ms, limit)
        self.tau = (burst - 1) * self.T
        self.tat: Fraction | None = None

    def allow(self, now: int) -> bool:
        tat = now if self.tat is None else max(self.tat, now)
        if tat - now > self.tau:
            return False
        self.tat = tat + self.T
        return True


class LeakyBucketMeter:
    """Leaky bucket used as a meter (policing): water level drains at the rate,
    each request adds one unit, overflow is rejected. No queue, no delay."""

    def __init__(self, capacity: int = 20, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.capacity = capacity
        self.rate = Fraction(limit, window_ms)
        self.level = Fraction(0)
        self.last: int | None = None

    def allow(self, now: int) -> bool:
        if self.last is not None:
            self.level = max(Fraction(0), self.level - (now - self.last) * self.rate)
        self.last = now
        if self.level + 1 <= self.capacity:
            self.level += 1
            return True
        return False


class LeakyBucketQueue:
    """Leaky bucket used as a queue (shaping): requests wait in a queue of up to
    `capacity` and leave it one every T ms. offer() returns the time the request
    leaves the queue and reaches the backend, or None if the queue was full."""

    def __init__(self, capacity: int = 20, limit: int = LIMIT, window_ms: int = WINDOW_MS):
        self.capacity = capacity
        self.rate = Fraction(limit, window_ms)
        self.level = Fraction(0)      # requests still in the bucket, in drain time
        self.last: int | None = None

    def offer(self, now: int) -> Fraction | None:
        if self.last is not None:
            self.level = max(Fraction(0), self.level - (now - self.last) * self.rate)
        self.last = now
        if self.level > self.capacity:
            return None
        release = now + self.level / self.rate
        self.level += 1
        return release


# ---------------------------------------------------------------- measuring

def max_in_span(times: list, span_ms: int) -> int:
    """Most timestamps inside any half-open span [s, s + span_ms)."""
    times = sorted(times)
    best = 0
    for i, t in enumerate(times):
        j = bisect.bisect_left(times, t + span_ms, lo=i)
        best = max(best, j - i)
    return best


def run(limiter, arrivals: list[int]) -> list:
    """Feed arrivals to a limiter; return when each allowed request reached the backend."""
    if isinstance(limiter, LeakyBucketQueue):
        released = (limiter.offer(t) for t in arrivals)
        return [r for r in released if r is not None]
    return [t for t in arrivals if limiter.allow(t)]


def all_limiters(burst: int = 20) -> dict[str, object]:
    """The seven bars in the chart: six algorithms, the token bucket at two burst sizes."""
    return {
        "Fixed window": FixedWindow(),
        "Sliding window log": SlidingLog(),
        "Sliding window counter": SlidingCounter(),
        f"Token bucket (B = {burst})": TokenBucket(capacity=burst),
        "Token bucket (B = 100)": TokenBucket(capacity=100),
        f"GCRA (B = {burst})": GCRA(burst=burst),
        f"Leaky bucket (queue of {burst})": LeakyBucketQueue(capacity=burst),
    }


# ---------------------------------------------------------------- traffic

def edge_burst() -> list[int]:
    """Idle, then 200 requests 5 ms apart from 59.5 s to 60.495 s: 100 on each
    side of the minute boundary."""
    return [59_500 + 5 * i for i in range(200)]


def steady(per_minute: int = 300, start: int = 0, end: int = 300_000) -> list[int]:
    """Evenly spaced traffic, 3x the limit by default (one request every 200 ms)."""
    gap = WINDOW_MS // per_minute
    return list(range(start, end, gap))


def edge_burst_then_steady() -> list[int]:
    """The main pattern: the edge burst, then 3x the limit for four more minutes."""
    return edge_burst() + steady(start=60_600, end=300_000)


def random_bursts(seed: int, minutes: int = 5, per_minute: int = 300, burst: int = 50) -> list[int]:
    """Bursty traffic averaging `per_minute`: bursts of `burst` requests, 10 ms
    apart, starting at random moments."""
    rng = random.Random(seed)
    n_bursts = minutes * per_minute // burst
    starts = sorted(rng.randrange(0, minutes * WINDOW_MS) for _ in range(n_bursts))
    return sorted(s + 10 * i for s in starts for i in range(burst))


PATTERNS = {
    "edge burst": edge_burst,
    "edge burst, then 3x for 4 min": edge_burst_then_steady,
    "steady 3x for 5 min": steady,
}
