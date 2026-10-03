"""A toy connection pool on asyncio streams. One line goes out, one line comes back, and
nothing in a reply says which request it answers: only its place in the queue does."""
import asyncio

Conn = tuple[asyncio.StreamReader, asyncio.StreamWriter]


class Pool:
    def __init__(self, host: str, port: int, size: int = 10):
        self.host, self.port = host, port
        self.slots = asyncio.Semaphore(size)  # at most `size` connections checked out
        self.idle: list[Conn] = []

    async def acquire(self) -> Conn:
        await self.slots.acquire()
        if self.idle:
            return self.idle.pop()  # last in, first out, like redis-py's pool
        try:
            return await asyncio.open_connection(self.host, self.port)
        except BaseException:
            self.slots.release()
            raise

    def release(self, conn: Conn) -> None:
        self.idle.append(conn)
        self.slots.release()

    def discard(self, conn: Conn) -> None:
        conn[1].close()  # a reply still on its way dies with the socket
        self.slots.release()

    def close(self) -> None:
        while self.idle:
            self.idle.pop()[1].close()


async def send_and_read(conn: Conn, request: str) -> str:
    reader, writer = conn
    writer.write(request.encode() + b"\n")
    await writer.drain()
    return (await reader.readuntil(b"\n")).decode().rstrip("\n")


# Instead of this
async def query_buggy(pool: Pool, request: str) -> str:
    conn = await pool.acquire()
    try:
        return await send_and_read(conn, request)
    finally:
        pool.release(conn)  # also runs on a cancel, with the reply still unread


# Use this
async def query(pool: Pool, request: str) -> str:
    conn = await pool.acquire()
    try:
        reply = await send_and_read(conn, request)
    except BaseException:  # CancelledError is a BaseException: `except Exception` misses it
        pool.discard(conn)
        raise
    pool.release(conn)
    return reply
