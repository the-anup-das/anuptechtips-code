"""What does the installed redis-py send when it opens a connection, before your command?

Every exchange here is one more round trip that a read pays when it lands on a connection
the fix has closed. A plain forwarding proxy records each chunk the client sends; the last
chunk is the GET itself. Prints one JSON line:

    python handshake.py
    python handshake.py --lean     # protocol=2, driver_info=None, as load_test.py --lean-handshake
    python handshake.py --lean --save   # also write results/handshake_lean.json
"""
import argparse
import asyncio
import json
import pathlib

import redis as redis_sync
import redis.asyncio as redis

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 56379, 9


def commands(chunk: bytes) -> list[str]:
    """The command names in one chunk of RESP: 'HELLO 3', 'CLIENT SETINFO', 'SELECT 9', ..."""
    names = []
    for part in chunk.split(b"*")[1:]:  # each command is an array: *<n>\r\n$<len>\r\n<arg>...
        args = part.split(b"\r\n")[2::2]
        names.append(b" ".join(args[:2]).decode())
    return names


async def main(lean: bool = False) -> dict:
    sent: list[list[str]] = []

    async def forward(reader, writer, record: bool) -> None:
        try:
            while data := await reader.read(65536):
                if record:
                    sent.append(commands(data))
                writer.write(data)
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    async def handle(reader, writer) -> None:
        target_reader, target_writer = await asyncio.open_connection(REDIS_HOST, REDIS_PORT)
        await asyncio.gather(forward(reader, target_writer, True),
                             forward(target_reader, writer, False))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    options = {"protocol": 2, "driver_info": None} if lean else {}  # RESP2, no CLIENT SETINFO
    r = redis.Redis(host="127.0.0.1", port=server.sockets[0].getsockname()[1], db=REDIS_DB,
                    **options)
    await r.get("handshake:none")
    await r.connection_pool.disconnect()
    server.close()
    return {"redis_py": redis_sync.__version__, "options": "protocol=2, driver_info=None" if lean
            else "defaults", "sent_before_the_command": sent[:-1],
            "round_trips_before_the_command": len(sent) - 1, "the_command": sent[-1]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lean", action="store_true")
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(main(args.lean))
    print(json.dumps(result))
    if args.save:
        name = "handshake_lean.json" if args.lean else "handshake.json"
        path = pathlib.Path(__file__).resolve().parent / "results" / name
        path.write_text(json.dumps(result, indent=2) + "\n", newline="\n")
