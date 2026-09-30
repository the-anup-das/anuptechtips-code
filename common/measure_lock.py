"""Serialise benchmark runs across processes, so another benchmark running at the
same time doesn't skew the timings. mkdir is atomic on Windows and POSIX.

    from common.measure_lock import measuring
    with measuring("outbox-throughput"):
        run_benchmark()
"""
import contextlib
import os
import pathlib
import time

LOCK = pathlib.Path(__file__).resolve().parent.parent / ".measure.lock"


@contextlib.contextmanager
def measuring(name, stale_after=3600, poll=2.0):
    while True:
        try:
            os.mkdir(LOCK)
            break
        except FileExistsError:
            try:
                if time.time() - LOCK.stat().st_mtime > stale_after:
                    (LOCK / "owner.txt").unlink(missing_ok=True)
                    os.rmdir(LOCK)
                    continue
            except (FileNotFoundError, OSError):
                continue
            time.sleep(poll)
    (LOCK / "owner.txt").write_text(f"{name} pid={os.getpid()} since={time.ctime()}\n")
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            (LOCK / "owner.txt").unlink()
        with contextlib.suppress(OSError):
            os.rmdir(LOCK)
