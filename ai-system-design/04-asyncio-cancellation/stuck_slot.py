"""The second shape of the bug: a cancel between "connection taken" and "connection given
back". One word is different from toy_pool.query: Exception instead of BaseException.
A cancel now skips both the discard and the release, so the slot never comes back."""
from toy_pool import Pool, send_and_read


async def query_leaky(pool: Pool, request: str) -> str:
    conn = await pool.acquire()
    try:
        reply = await send_and_read(conn, request)
    except Exception:  # a CancelledError flies straight past this...
        pool.discard(conn)
        raise
    pool.release(conn)  # ...and never gets here: the slot is gone for good
    return reply
