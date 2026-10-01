"""kill -9 a relay worker mid-run: nothing is lost, and the duplicates stay bounded.

On Windows, Popen.kill() is TerminateProcess: no cleanup handlers, no COMMIT,
the same as SIGKILL on Linux. Postgres sees the socket close, rolls the
worker's transaction back and releases its row locks.
"""
import random
import time

from conftest import pending
from harness import count_sent, finish, go, seed, spawn

BATCH = 100


def test_kill_9_loses_nothing_and_duplicates_at_most_a_batch_per_kill(conn, tmp_path):
    seed(conn, 100_000)
    rng = random.Random(7)
    logs, n = [], 0

    def start(wait_for_go=False):
        nonlocal n
        n += 1
        log = tmp_path / f"incarnation-{n}.log"  # a fresh file per process: a kill can cut a line
        logs.append(log)
        return spawn(batch=BATCH, log=log, wait_for_go=wait_for_go)

    workers = [start(wait_for_go=True) for _ in range(4)]
    go(workers)
    kills = 0
    while kills < 5:
        time.sleep(rng.uniform(0.05, 0.3))
        i = rng.randrange(len(workers))
        if workers[i].poll() is not None:
            break  # the work ran out before we got to kill it
        workers[i].kill()
        workers[i].wait()
        kills += 1
        workers[i] = start()

    for w in workers:
        finish(w)
    # a worker that exited just before its restarted peer could leave rows; mop up
    finish(spawn(batch=BATCH, log=(tmp_path / "mop-up.log"), wait_for_go=False))
    logs.append(tmp_path / "mop-up.log")

    sent = count_sent(logs)
    all_ids = {str(r[0]) for r in conn.execute("SELECT id FROM outbox")}
    duplicates = sum(c - 1 for c in sent.values())
    print(f"kills={kills} duplicates={duplicates}")

    assert kills >= 3
    assert pending(conn) == 0
    assert set(sent) == all_ids  # 0 missing
    assert duplicates <= kills * BATCH  # each kill re-sends at most the batch it held
