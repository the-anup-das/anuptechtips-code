"""Cancel a GET after it was sent, then GET another key (after redis-py issue #2665).

A delay proxy holds every chunk for 100 ms each way, so the first GET is still on the
wire when it is cancelled 50 ms in. Run it with the redis-py you have installed:

    python repro_cancel_then_get.py

redis-py 4.5.1 prints `bar: b'foo'`: the second GET got the first one's reply.
redis-py 4.5.5 and later print `bar: b'bar'`.

    python repro_cancel_then_get.py --pause-ms 300

waits 300 ms between the cancel and the second GET, so the stale reply has arrived by then.
Now redis-py 4.5.1 prints `bar: b'bar'` too: its pool looks for unread data when it hands a
connection out, finds the reply and reconnects. The check can't see a reply that is still
on the wire, which is the case above.
"""
import argparse
import asyncio
import json

import redis as redis_sync
import redis.asyncio as redis

from delay_proxy import DelayProxy
from precise_loop import run

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 56379, 9


async def main(pause_ms: float = 0) -> dict:
    proxy = DelayProxy(REDIS_HOST, REDIS_PORT, delay=0.1)
    r = redis.Redis(host="127.0.0.1", port=await proxy.start(), db=REDIS_DB)
    await r.set("repro:foo", "foo")
    await r.set("repro:bar", "bar")

    task = asyncio.create_task(r.get("repro:foo"))
    await asyncio.sleep(0.05)  # the GET is on the wire; its reply is 150 ms away
    task.cancel()
    try:
        await task
        print("the GET finished before the cancel: run it again")
    except asyncio.CancelledError:
        print("GET repro:foo cancelled after 50 ms: sent, reply not read")
    await asyncio.sleep(pause_ms / 1000)

    result = {"redis_py": redis_sync.__version__,
              "bar": repr(await r.get("repro:bar")),
              "ping": repr(await r.ping()),
              "foo": repr(await r.get("repro:foo"))}
    for name in ("bar", "ping", "foo"):
        print(f"{name}: {result[name]}")
    result["connections_opened"] = proxy.connections

    await r.connection_pool.disconnect()
    proxy.close()
    direct = redis_sync.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    direct.delete("repro:foo", "repro:bar")  # not through r: it may still be off by one
    direct.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause-ms", type=float, default=0,
                        help="wait this long between the cancel and the second GET")
    parser.add_argument("--json", action="store_true", help="also print the result as JSON")
    args = parser.parse_args()
    outcome = run(main(args.pause_ms))
    if args.json:
        print(json.dumps({**outcome, "pause_ms": args.pause_ms}))
