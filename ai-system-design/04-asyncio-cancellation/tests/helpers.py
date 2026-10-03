"""Shared test helpers: a held toy server, a mid-flight cancel, and a load_test.py runner."""
import asyncio
import contextlib
import json
import os
import pathlib
import subprocess
import sys

import pytest

from toy_pool import Pool
from toy_server import ToyServer

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 56379, 9
FOLDER = pathlib.Path(__file__).resolve().parent.parent
OLD_PYTHON = os.environ.get("REDIS_451_PYTHON")  # a Python with redis==4.5.1, if you made one


@contextlib.asynccontextmanager
async def held_toy(size: int = 1):
    """A toy server that holds every reply until `server.hold` is set, and a pool on it."""
    server = ToyServer()
    server.hold = asyncio.Event()
    pool = Pool("127.0.0.1", await server.start(), size=size)
    try:
        yield server, pool
    finally:
        server.hold.set()
        pool.close()
        server.close()
        await asyncio.sleep(0.01)  # let the transports finish closing


async def cancel_mid_flight(server: ToyServer, pool: Pool, query_fn, request: str = "user:1"):
    """Start a query and cancel it once the server has the request: sent, reply pending."""
    task = asyncio.create_task(query_fn(pool, request))
    assert await server.received.get() == request
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def user_id_of(reply: str) -> int:
    return json.loads(reply)["user_id"]


def load_test(*args: str, python: str = sys.executable) -> dict:
    """Run load_test.py in a fresh process and return its JSON line."""
    done = subprocess.run([python, "load_test.py", *args], cwd=FOLDER, capture_output=True,
                          text=True, timeout=120)
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])
