"""Side check: round-trip time vs the size of the message the client sends.

On Docker Desktop for Windows, messages over about 8 KB sent to the published
port pick up a fixed extra delay. This is why the relay claims and marks in one
statement instead of sending a list of ids back.

    python measure_message_size.py
"""
from __future__ import annotations

import json
import pathlib
import statistics
import sys
import time

import psycopg

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common.measure_lock import measuring  # noqa: E402
from db import DSN  # noqa: E402

SIZES = [1_000, 4_000, 8_000, 9_000, 10_000, 12_000, 16_000, 32_000, 64_000]


def main() -> None:
    out = {}
    with measuring("outbox-message-size"), psycopg.connect(DSN, autocommit=True) as conn:
        for size in SIZES:
            param = "x" * size
            times = []
            for _ in range(20):
                t = time.perf_counter()
                conn.execute("SELECT length(%s)", (param,)).fetchone()
                times.append((time.perf_counter() - t) * 1000)
            out[size] = {"median_ms": round(statistics.median(times), 2),
                         "min_ms": round(min(times), 2), "max_ms": round(max(times), 2)}
            print(size, out[size], flush=True)
    (HERE / "results" / "net_message_size.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
