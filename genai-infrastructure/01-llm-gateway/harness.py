"""Helpers shared by the tests and the measurement scripts: run an ASGI app on a free
port, in a thread or as its own process, and summarise latencies."""
import contextlib
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator

import httpx
import uvicorn

HERE = os.path.dirname(os.path.abspath(__file__))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def serve(app, log_level: str = "critical") -> Iterator[str]:
    """Run an ASGI app under uvicorn in a background thread. Yields its base URL."""
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level=log_level,
                                           timeout_keep_alive=600, timeout_graceful_shutdown=1))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError("the server failed to start")
        time.sleep(0.01)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@contextlib.contextmanager
def spawn(app: str, env: dict[str, str] | None = None, workers: int = 1,
          log_name: str = "server") -> Iterator[str]:
    """Run `uvicorn <module:app>` as a separate process on a free port, so a benchmark's
    client, gateway and mock provider don't share one interpreter. Yields its base URL."""
    port = free_port()
    log_path = os.path.join(HERE, "results", f"{log_name}.log")
    windows = sys.platform == "win32"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", app, "--host", "127.0.0.1", "--port", str(port),
             "--workers", str(workers), "--log-level", "warning", "--no-access-log",
             "--timeout-keep-alive", "600"],
            cwd=HERE, env={**os.environ, **(env or {})}, stdout=log, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if windows else 0)
        url = f"http://127.0.0.1:{port}"
        try:
            for _ in range(200):
                try:
                    httpx.get(f"{url}/openapi.json", timeout=1)
                    break
                except httpx.TransportError:
                    if proc.poll() is not None:
                        raise RuntimeError(f"{app} exited: see {log_path}") from None
                    time.sleep(0.1)
            else:
                raise RuntimeError(f"{app} did not start: see {log_path}")
            yield url
        finally:
            # Ctrl+Break on Windows, SIGTERM elsewhere: uvicorn then stops its workers too.
            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT if windows else signal.SIGTERM)
                proc.wait(timeout=10)
            except (OSError, SystemError, subprocess.TimeoutExpired):
                # Ctrl+Break needs a console. Without one, os.kill() fails on Windows, and
                # CPython reports that as a SystemError: stop the process tree the hard way.
                if windows:  # kill() would leave worker processes behind
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                                   capture_output=True)
                else:
                    proc.kill()


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile: the smallest value with at least p% of the samples at or below it."""
    ordered = sorted(values)
    rank = max(1, -(-len(ordered) * p // 100))  # ceil without importing math
    return ordered[int(rank) - 1]
