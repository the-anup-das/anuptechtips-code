"""Atomic Redis rate limiters: six Lua scripts in lua/, called by hash with
EVALSHA (redis-py's register_script) or loaded once as a Redis Functions
library and called with FCALL. Plus the two non-Lua versions the post compares
them with: the racy read-then-write, and a MULTI/EXEC fixed window.
"""
import pathlib
import uuid

import redis

LUA = pathlib.Path(__file__).parent / "lua"
ALGORITHMS = ("fixed_window", "sliding_log", "sliding_counter",
              "token_bucket", "leaky_bucket", "gcra")
LIMIT, WINDOW, BURST = 100, 60, 20          # 100 requests a minute, bursts of 20


def connect(**kw) -> redis.Redis:
    """The post's Redis: DB 2 on the Docker service from docker-compose.yml."""
    return redis.Redis(host="localhost", port=56379, db=2, **kw)


def lua_source(name: str) -> str:
    return (LUA / f"{name}.lua").read_text()


def args_for(name: str, limit: int = LIMIT, window: float = WINDOW, burst: int = BURST) -> list:
    """ARGV for each script at `limit` requests per `window` seconds."""
    rate = limit / window
    return {
        "fixed_window": [limit, window],
        "sliding_log": [limit, window, uuid.uuid4().hex],   # unique ZSET member
        "sliding_counter": [limit, window],
        "token_bucket": [rate, burst, 1],
        "leaky_bucket": [rate, burst],
        "gcra": [window / limit, burst],
    }[name]


def register_all(r: redis.Redis) -> dict:
    """name -> redis-py Script. Calling one runs EVALSHA and reloads on NOSCRIPT."""
    return {name: r.register_script(lua_source(name)) for name in ALGORITHMS}


def function_library() -> str:
    """The same six scripts as one Redis Functions library (Redis 7+).
    Naming the callback's parameters KEYS and ARGV lets each body run unchanged."""
    parts = ["#!lua name=ratelimit\n"]
    for name in ALGORITHMS:
        parts.append(f"redis.register_function('rl_{name}', function(KEYS, ARGV)\n"
                     f"{lua_source(name)}\nend)\n")
    return "".join(parts)


def load_functions(r: redis.Redis) -> None:
    r.function_load(function_library(), replace=True)


def fcall(r: redis.Redis, name: str, key: str, args: list) -> list:
    return r.fcall(f"rl_{name}", 1, key, *args)


def naive_fixed_window(r: redis.Redis, key: str, limit: int = LIMIT, window: int = WINDOW) -> bool:
    """The race you'll write first. Don't use it."""
    count = int(r.get(key) or 0)          # 1. read
    if count >= limit:                    # 2. decide in Python
        return False
    if r.incr(key) == 1:                  # 3. write: others may have written since 1.
        r.expire(key, window)             #    and a crash here leaves no TTL
    return True


def fixed_window_multi(r: redis.Redis, key: str, limit: int = LIMIT, window: int = WINDOW) -> bool:
    """INCR and EXPIRE NX in one MULTI/EXEC: both run or neither does."""
    pipe = r.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, window, nx=True)     # set the TTL only on the first request
    count, _ = pipe.execute()
    return count <= limit
