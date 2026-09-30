"""Start relay_worker.py processes and count what they sent. Used by tests/ and measure_*.py."""
from __future__ import annotations

import collections
import pathlib
import subprocess
import sys

import psycopg

HERE = pathlib.Path(__file__).resolve().parent


def seed(conn: psycopg.Connection, n: int, aggregates: int | None = None) -> None:
    """Insert n pending events in one statement (aggregates: how many distinct aggregateids)."""
    conn.execute(
        "INSERT INTO outbox (aggregatetype, aggregateid, type, payload) "
        "SELECT 'order', 'order-' || (g %% %s), 'OrderPlaced', jsonb_build_object('n', g) "
        "FROM generate_series(1, %s) AS g",
        (aggregates or n, n))


def spawn(batch: int, log: pathlib.Path | None = None, publisher: str = "stub",
          topic_prefix: str | None = None, wait_for_go: bool = True,
          exit_when_empty: bool = True) -> subprocess.Popen:
    cmd = [sys.executable, str(HERE / "relay_worker.py"), "--batch", str(batch),
           "--publisher", publisher]
    if log:
        cmd += ["--log", str(log)]
    if topic_prefix:
        cmd += ["--topic-prefix", topic_prefix]
    if wait_for_go:
        cmd.append("--wait-for-go")
    if exit_when_empty:
        cmd.append("--exit-when-empty")
    proc = subprocess.Popen(cmd, cwd=HERE, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True, bufsize=1)
    line = proc.stdout.readline().strip()
    if line != "READY":
        proc.kill()
        raise RuntimeError(f"worker didn't start: {line!r}")
    return proc


def go(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.stdin.write("go\n")
        p.stdin.flush()


def finish(proc: subprocess.Popen, timeout: float = 300) -> tuple[int, float, float]:
    """Wait for a worker started with exit_when_empty; return (rows, first, last)."""
    out, _ = proc.communicate(timeout=timeout)
    done = [line for line in out.splitlines() if line.startswith("DONE")]
    if proc.returncode != 0 or not done:
        raise RuntimeError(f"worker failed (exit {proc.returncode}): {out[-500:]}")
    _, rows, first, last = done[-1].split()
    return int(rows), float(first), float(last)


def count_sent(logs: list[pathlib.Path]) -> collections.Counter:
    """How many times each id reached the stub 'broker'."""
    sent: collections.Counter = collections.Counter()
    for path in logs:
        if path.exists():
            # A kill can cut the last line short; a partial id can't match a real one.
            sent.update(line for line in path.read_text(encoding="ascii").splitlines()
                        if len(line) == 36)
    return sent
