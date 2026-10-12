import threading
import time

import pytest

from cache_aside import Cache


def test_stampede_hits_database_once():
    calls = []

    def slow_load(key):
        calls.append(key)
        time.sleep(0.2)
        return f"value-{key}"

    cache = Cache(slow_load, ttl=30)
    results = []
    threads = [threading.Thread(target=lambda: results.append(cache.get("hot"))) for _ in range(200)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(calls) == 1
    assert results == ["value-hot"] * 200


def test_error_serves_old_value_and_backs_off():
    calls = []
    healthy = True

    def load(key):
        calls.append(key)
        if not healthy:
            raise ConnectionError("database overloaded")
        return "v1"

    cache = Cache(load, ttl=0.1, grace=0.5)
    assert cache.get("k") == "v1"
    time.sleep(0.2)                   # entry expires
    healthy = False

    assert cache.get("k") == "v1"     # the error is not treated as a new value
    for _ in range(100):              # within the grace period: no more DB calls
        assert cache.get("k") == "v1"
    assert len(calls) == 2

    time.sleep(0.6)
    healthy = True
    assert cache.get("k") == "v1"
    assert len(calls) == 3            # retried only after the grace period


def test_error_without_old_value_raises():
    def load(key):
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        Cache(load).get("missing")
