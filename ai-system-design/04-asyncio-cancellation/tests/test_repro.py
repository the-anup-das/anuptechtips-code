"""repro_cancel_then_get.py: a GET cancelled mid-flight, then a GET of another key."""
import json
import subprocess
import sys

import pytest

from helpers import FOLDER, OLD_PYTHON


def repro(python: str, *args: str) -> dict:
    done = subprocess.run([python, "repro_cancel_then_get.py", "--json", *args], cwd=FOLDER,
                          capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr[-2000:]
    assert "sent, reply not read" in done.stdout  # the cancel landed mid-flight
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_installed_redis_py_answers_each_get_with_its_own_value(db):
    result = repro(sys.executable)
    assert (result["bar"], result["ping"], result["foo"]) == ("b'bar'", "True", "b'foo'")
    assert result["connections_opened"] == 2  # the cancelled connection was closed


needs_old_redis_py = pytest.mark.skipif(
    not OLD_PYTHON, reason="set REDIS_451_PYTHON to a Python with redis==4.5.1 (see README)")


@needs_old_redis_py
def test_redis_py_4_5_1_answers_the_second_get_with_the_first_ones_value(db):
    result = repro(OLD_PYTHON)
    assert result["redis_py"] == "4.5.1"
    assert (result["bar"], result["ping"], result["foo"]) == ("b'foo'", "False", "b'PONG'")
    assert result["connections_opened"] == 1  # still the same, poisoned connection


@needs_old_redis_py
def test_redis_py_4_5_1_notices_a_stale_reply_that_has_already_arrived(db):
    """Its pool checks for unread data at checkout, so only a reply still on the wire leaks."""
    result = repro(OLD_PYTHON, "--pause-ms", "300")
    assert (result["bar"], result["ping"], result["foo"]) == ("b'bar'", "True", "b'foo'")
    assert result["connections_opened"] == 2  # the pool reconnected
