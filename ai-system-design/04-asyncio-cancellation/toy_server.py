"""The other end of the toy protocol: a cache that answers `user:<id>` with that user's
profile as one line of JSON. Like Redis, it answers every request it receives, in order,
whether or not the client that sent it is still waiting."""
import asyncio
import json


def profile_for(user_id: int) -> dict:
    return {"user_id": user_id, "email": f"user{user_id}@example.com", "plan": "plus"}


class ToyServer:
    def __init__(self) -> None:
        self.hold: asyncio.Event | None = None  # tests: replies wait until this is set
        self.received: asyncio.Queue[str] = asyncio.Queue()  # each request, as it arrives
        self.connections = 0  # client connections accepted so far
        self.port = 0

    async def start(self, port: int = 0) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        try:
            while line := await reader.readline():
                request = line.decode().strip()
                self.received.put_nowait(request)
                if self.hold is not None:
                    await self.hold.wait()
                user_id = int(request.removeprefix("user:"))
                writer.write(json.dumps(profile_for(user_id)).encode() + b"\n")
                await writer.drain()
        except ConnectionError:
            pass  # the client closed the connection: that's the fix doing its job
        finally:
            writer.close()

    def close(self) -> None:
        self._server.close()
