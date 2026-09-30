"""Against the Docker Redis (localhost:56379, DB 2). Uses unique keys per test."""
import threading
import time
import uuid

import pytest
import redis

import redis_limiters as rl


@pytest.fixture(scope="module")
def r():
    client = rl.connect()
    client.ping()
    rl.load_functions(client)
    yield client
    for key in client.scan_iter("test:*"):
        client.delete(key)


@pytest.fixture(scope="module")
def scripts(r):
    return rl.register_all(r)


@pytest.fixture
def key():
    return f"test:{uuid.uuid4().hex}"


BACKENDS = ["evalsha", "fcall"]


def call(backend, r, scripts, name, key, **kw):
    args = rl.args_for(name, **kw)
    if backend == "evalsha":
        return scripts[name](keys=[key], args=args)
    return rl.fcall(r, name, key, args)


def burst(backend, r, scripts, name, key, n, **kw):
    return [call(backend, r, scripts, name, key, **kw) for _ in range(n)]


# Hour-long windows keep a calendar-window boundary from landing mid-test.

@pytest.mark.parametrize("backend", BACKENDS)
def test_fixed_window(backend, r, scripts, key):
    out = burst(backend, r, scripts, "fixed_window", key, 110, window=3600)
    assert sum(a for a, _ in out) == 100
    assert out[-1][0] == 0 and 0 < out[-1][1] <= 3_600_000   # wait = time to the next window


@pytest.mark.parametrize("backend", BACKENDS)
def test_sliding_log(backend, r, scripts, key):
    out = burst(backend, r, scripts, "sliding_log", key, 110)
    assert sum(a for a, _ in out) == 100
    assert r.zcard(key) == 100
    assert 55_000 < out[-1][1] <= 60_000                    # the oldest leaves in ~60 s


@pytest.mark.parametrize("backend", BACKENDS)
def test_sliding_counter(backend, r, scripts, key):
    out = burst(backend, r, scripts, "sliding_counter", key, 110, window=3600)
    assert sum(a for a, _ in out) == 100
    assert out[-1][0] == 0 and out[-1][1] > 0


@pytest.mark.parametrize("backend", BACKENDS)
def test_token_bucket_burst_wait_refill(backend, r, scripts, key):
    out = burst(backend, r, scripts, "token_bucket", key, 21)
    assert [a for a, _ in out] == [1] * 20 + [0]
    assert 500 < out[-1][1] <= 600                          # one token every 600 ms
    time.sleep(out[-1][1] / 1000 + 0.02)
    assert call(backend, r, scripts, "token_bucket", key)[0] == 1


@pytest.mark.parametrize("backend", BACKENDS)
def test_gcra_burst_wait_refill(backend, r, scripts, key):
    out = burst(backend, r, scripts, "gcra", key, 21)
    assert [a for a, _ in out] == [1] * 20 + [0]
    assert 500 < out[-1][1] <= 600
    time.sleep(out[-1][1] / 1000 + 0.02)
    assert call(backend, r, scripts, "gcra", key)[0] == 1


@pytest.mark.parametrize("backend", BACKENDS)
def test_leaky_bucket_queues_then_rejects(backend, r, scripts, key):
    out = burst(backend, r, scripts, "leaky_bucket", key, 22)
    assert [a for a, _ in out] == [1] * 21 + [0]
    delays = [d for a, d in out if a]
    assert delays[0] == 0
    assert 11_500 < delays[-1] <= 12_000                    # 20 queued x 0.6 s
    assert all(b > a for a, b in zip(delays, delays[1:]))


@pytest.mark.parametrize("name", rl.ALGORITHMS)
def test_every_key_expires(name, r, scripts, key):
    call("evalsha", r, scripts, name, key)
    assert r.pttl(key) > 0


def test_redis_clock_going_backwards_does_not_drain_the_bucket(r, scripts, key):
    future = time.time() + 100                  # state written by a clock 100 s ahead
    r.hset(key, mapping={"tokens": 0, "ts": future})
    allowed, wait_ms = scripts["token_bucket"](keys=[key], args=rl.args_for("token_bucket"))
    assert allowed == 0 and wait_ms <= 600      # without math.max: wait ~100 s


def test_naive_limiter_races_past_the_limit(r, key):
    allowed = count_concurrent(lambda: rl.naive_fixed_window(r, key))
    assert allowed > 100


def test_lua_limiter_holds_under_the_same_race(r, scripts, key):
    fixed = scripts["fixed_window"]
    allowed = count_concurrent(lambda: fixed(keys=[key], args=[100, 3600])[0] == 1)
    assert allowed == 100


def test_multi_exec_fixed_window_holds_under_the_same_race(r, key):
    allowed = count_concurrent(lambda: rl.fixed_window_multi(r, key, window=3600))
    assert allowed == 100


def count_concurrent(attempt, threads: int = 20, per_thread: int = 25) -> int:
    start = threading.Barrier(threads)
    results = []

    def worker():
        start.wait()
        results.extend(attempt() for _ in range(per_thread))

    pool = [threading.Thread(target=worker) for _ in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    return sum(results)


def test_incr_then_expire_crash_leaves_a_key_that_never_expires(r, key):
    r.incr(key)                                 # ...and the process dies here
    assert r.ttl(key) == -1                     # no TTL: this client is stuck for good


def test_multi_exec_is_all_or_nothing(r, key):
    conn = r.connection_pool.get_connection()
    conn.send_command("MULTI")
    assert conn.read_response() == b"OK"
    conn.send_command("INCR", key)
    assert conn.read_response() == b"QUEUED"
    conn.disconnect()                           # the client dies before EXEC
    r.connection_pool.release(conn)
    assert r.exists(key) == 0                   # Redis threw the queued INCR away


def test_register_script_reloads_after_noscript(r, key):
    script = r.register_script(rl.lua_source("gcra") + f"\n-- {uuid.uuid4()}")
    assert r.script_exists(script.sha) == [False]
    with pytest.raises(redis.exceptions.NoScriptError):
        r.evalsha(script.sha, 1, key, *rl.args_for("gcra"))
    assert script(keys=[key], args=rl.args_for("gcra")) == [1, 0]
    assert r.script_exists(script.sha) == [True]


def test_lua_numbers_come_back_as_integers(r):
    assert r.eval("return 1.9", 0) == 1         # the decimal part is dropped
    assert r.eval("return tostring(1.9)", 0) == b"1.9"


def test_timestamp_members_collapse_in_a_sorted_set(r, key):
    for _ in range(150):                        # same millisecond, same member
        r.zadd(key, {"1759230000000": 1759230000000})
    assert r.zcard(key) == 1                    # 150 requests counted once


def test_functions_survive_script_cache_loss(r, key):
    # FCALL names the function; there is no SHA to go missing.
    assert b"ratelimit" in [lib[1] for lib in r.function_list(library="ratelimit")]
    assert rl.fcall(r, "gcra", key, rl.args_for("gcra")) == [1, 0]
