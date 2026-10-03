"""handshake.py: what a new connection costs before the first command."""
import json
import subprocess
import sys

import pytest

from helpers import FOLDER, OLD_PYTHON


def handshake(python: str, *args: str) -> dict:
    done = subprocess.run([python, "handshake.py", *args], cwd=FOLDER, capture_output=True,
                          text=True, timeout=60)
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_a_new_connection_costs_round_trips_before_the_command(db):
    result = handshake(sys.executable)
    assert result["the_command"] == ["GET handshake:none"]
    assert result["round_trips_before_the_command"] >= 1  # at least the one with SELECT 9
    assert result["sent_before_the_command"][-1][-1] == "SELECT 9"


def test_resp2_without_client_info_connects_with_select_only(db):
    """What load_test.py --lean-handshake uses: protocol=2, driver_info=None."""
    assert handshake(sys.executable, "--lean")["sent_before_the_command"] == [["SELECT 9"]]


@pytest.mark.skipif(not OLD_PYTHON,
                    reason="set REDIS_451_PYTHON to a Python with redis==4.5.1 (see README)")
def test_redis_py_4_5_1_connects_with_one_round_trip(db):
    assert handshake(OLD_PYTHON)["sent_before_the_command"] == [["SELECT 9"]]
