import os
import subprocess
import sys

import psycopg

from advisory import lock_id, try_lock
from conftest import HERE, PG_DSN


def test_second_connection_gets_false_until_the_first_transaction_ends():
    with psycopg.connect(PG_DSN) as a, psycopg.connect(PG_DSN) as b:
        with a.transaction():
            assert try_lock(a, "t:nightly-report") is True
            with b.transaction():
                assert try_lock(b, "t:nightly-report") is False
        with b.transaction():  # a committed, so the lock is gone
            assert try_lock(b, "t:nightly-report") is True


def test_rollback_releases_too():
    with psycopg.connect(PG_DSN) as a, psycopg.connect(PG_DSN) as b:
        try:
            with a.transaction():
                assert try_lock(a, "t:job")
                raise RuntimeError("job failed")
        except RuntimeError:
            pass
        with b.transaction():
            assert try_lock(b, "t:job") is True


def test_lock_id_is_stable_across_processes_but_hash_is_not():
    code = f"import sys; sys.path.insert(0, {str(HERE)!r}); from advisory import lock_id; " \
           "print(lock_id('nightly-report'), hash('nightly-report'))"
    outputs = []
    for seed in ("1", "2"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        outputs.append(subprocess.run([sys.executable, "-c", code], env=env,
                                      capture_output=True, text=True, check=True).stdout.split())
    assert outputs[0][0] == outputs[1][0] == str(lock_id("nightly-report"))
    assert outputs[0][1] != outputs[1][1]  # Python's hash() changes per process
