"""A TCP proxy that holds every chunk for `delay` seconds in each direction, so a request
spends real time on the wire and a timeout can land between "sent" and "reply read".
The idea comes from the reproduction script in redis-py issue #2665."""
import asyncio


class DelayProxy:
    def __init__(self, target_host: str, target_port: int, delay: float):
        self.target = (target_host, target_port)
        self.delay = delay
        self.connections = 0  # client connections accepted so far
        self.port = 0

    async def start(self, port: int = 0) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                await asyncio.sleep(self.delay)
                writer.write(data)
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        target_reader, target_writer = await asyncio.open_connection(*self.target)
        await asyncio.gather(self._pump(reader, target_writer),
                             self._pump(target_reader, writer))

    def close(self) -> None:
        self._server.close()
