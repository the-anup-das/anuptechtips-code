import threading
import time


class Cache:
    """Cache-aside: one refill per key, and an error is never treated as data."""

    def __init__(self, load, ttl=30.0, grace=5.0):
        self.load = load      # reads the database
        self.ttl = ttl        # how stale a value may get
        self.grace = grace    # how long to keep an old value after a failed refill
        self.entries = {}     # key -> (value, expires_at)
        self.locks = {}
        self.guard = threading.Lock()

    def get(self, key):
        entry = self.entries.get(key)
        if entry and entry[1] > time.monotonic():
            return entry[0]                  # fresh hit

        with self.guard:
            lock = self.locks.setdefault(key, threading.Lock())
        with lock:                           # one caller refills, the rest wait
            entry = self.entries.get(key)
            if entry and entry[1] > time.monotonic():
                return entry[0]              # someone refilled it while we waited
            try:
                value = self.load(key)
            except Exception:
                if entry is None:
                    raise                    # nothing old to fall back on
                # The database failed. Keep the old value a little longer,
                # so the next caller doesn't hit the database straight away.
                self.entries[key] = (entry[0], time.monotonic() + self.grace)
                return entry[0]
            self.entries[key] = (value, time.monotonic() + self.ttl)
            return value
