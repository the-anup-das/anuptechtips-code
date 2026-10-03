"""The in-process token bucket from the rate limiting post (part 2 of the Reliable Python
services series). Here it is the fallback for when the shared limiter can't answer."""
import threading
import time
from typing import Callable


class TokenBucket:
    def __init__(self, rate: float, capacity: float,
                 clock: Callable[[], float] = time.monotonic):
        self.rate = rate              # tokens added per second
        self.capacity = capacity      # biggest burst allowed
        self._clock = clock           # pass a fake clock in tests
        self._tokens = capacity       # start full
        self._last = clock()
        self._lock = threading.Lock()

    def try_acquire(self, cost: float = 1.0) -> float:
        """Return 0.0 if allowed, otherwise seconds to wait."""
        if cost > self.capacity:
            raise ValueError("cost is bigger than the bucket")
        with self._lock:
            now = self._clock()
            refill = (now - self._last) * self.rate
            self._tokens = min(self.capacity, self._tokens + refill)
            self._last = now
            if self._tokens >= cost:
                self._tokens -= cost
                return 0.0
            return (cost - self._tokens) / self.rate
