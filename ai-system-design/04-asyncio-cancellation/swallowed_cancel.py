"""The third shape: code that catches CancelledError and carries on. The caller's timeout
fires, the cancel is absorbed, and the call takes as long as the pool's own timeout."""
import asyncio

from toy_pool import Conn, Pool

POOL_TIMEOUT = 1.0  # the pool's own patience, in seconds


# Instead of this
async def getconn_swallowing(pool: Pool) -> Conn:
    deadline = asyncio.get_running_loop().time() + POOL_TIMEOUT
    while True:
        try:
            remaining = deadline - asyncio.get_running_loop().time()
            return await asyncio.wait_for(pool.acquire(), remaining)
        except asyncio.CancelledError:
            continue  # "retry": the caller's cancel is gone, and so is its timeout


# Use this
async def getconn(pool: Pool) -> Conn:
    try:
        return await asyncio.wait_for(pool.acquire(), POOL_TIMEOUT)
    except asyncio.CancelledError:
        # Clean up here if there is anything to clean up, then always let it through.
        raise
