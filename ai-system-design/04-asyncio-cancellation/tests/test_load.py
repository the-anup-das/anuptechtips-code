"""Short runs of load_test.py: the same comparison as the full measurement, at 1,500 reads."""
import pytest

from helpers import OLD_PYTHON, load_test

SMALL = ["--requests", "1500", "--tasks", "100", "--think-ms", "300"]


def test_buggy_toy_pool_returns_other_users_profiles_under_load():
    result = load_test("--client", "toy-buggy", *SMALL)
    assert result["wrong_owner"] > 0
    assert result["connections_opened"] <= 10  # it never closed a connection


def test_fixed_toy_pool_returns_none():
    result = load_test("--client", "toy-fixed", *SMALL)
    assert result["wrong_owner"] == 0
    assert result["timed_out"] > 0             # requests were cancelled...
    assert result["connections_opened"] > 10   # ...and their connections were closed
    assert result["correct"] + result["timed_out"] == result["requests"]


def test_owner_check_turns_the_toy_pools_leaks_into_counted_misses():
    result = load_test("--client", "toy-buggy", "--owner-check", *SMALL)
    assert result["wrong_owner"] == 0
    assert result["owner_mismatches_caught"] > 0
    assert result["misses"] == result["owner_mismatches_caught"]


def test_installed_redis_py_returns_none(db):
    result = load_test("--client", "redis", *SMALL)
    assert result["wrong_owner"] == 0
    assert result["timed_out"] > 0
    assert result["connections_opened"] > 10
    assert result["errors"] == 0


def test_a_one_round_trip_handshake_changes_the_cost_not_the_safety(db):
    result = load_test("--client", "redis", "--lean-handshake", *SMALL)
    assert result["wrong_owner"] == 0
    assert result["timed_out"] > 0
    assert result["connections_opened"] > 10
    assert result["errors"] == 0


def test_nothing_is_cancelled_without_timeouts(db):
    result = load_test("--client", "redis", "--no-timeouts", *SMALL)
    assert result["correct"] == result["requests"]
    assert result["connections_opened"] <= 10


needs_old_redis_py = pytest.mark.skipif(
    not OLD_PYTHON, reason="set REDIS_451_PYTHON to a Python with redis==4.5.1 (see README)")


@needs_old_redis_py
def test_redis_py_4_5_1_returns_other_users_profiles_under_load(db):
    result = load_test("--client", "redis", *SMALL, python=OLD_PYTHON)
    assert result["library"] == "redis-py 4.5.1"
    assert result["wrong_owner"] > 0


@needs_old_redis_py
def test_owner_check_catches_every_leak_of_redis_py_4_5_1(db):
    result = load_test("--client", "redis", "--owner-check", *SMALL, python=OLD_PYTHON)
    assert result["wrong_owner"] == 0
    assert result["owner_mismatches_caught"] > 0
