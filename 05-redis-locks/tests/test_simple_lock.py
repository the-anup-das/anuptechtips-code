import time

import pytest

from simple_lock import acquire, release

BOTH = pytest.mark.parametrize("use_delex", [True, False], ids=["DELEX", "Lua"])


def test_second_acquire_fails_while_held(r):
    assert acquire(r, "t:job", ttl_ms=5000, wait_s=0) is not None
    started = time.monotonic()
    assert acquire(r, "t:job", ttl_ms=5000, wait_s=0.3) is None
    assert time.monotonic() - started >= 0.3  # it retried until the deadline


def test_set_nx_px_sets_value_and_expiry_together(r):
    token = acquire(r, "t:job", ttl_ms=5000, wait_s=0)
    assert r.get("t:job").decode() == token
    assert 0 < r.pttl("t:job") <= 5000


def test_tokens_are_unique_per_holder(r):
    tokens = set()
    for _ in range(50):
        tokens.add(acquire(r, "t:job", ttl_ms=5000, wait_s=0))
        r.delete("t:job")
    assert len(tokens) == 50


@BOTH
def test_release_with_wrong_token_returns_false_and_keeps_lock(r, use_delex):
    token = acquire(r, "t:job", ttl_ms=5000, wait_s=0)
    assert release(r, "t:job", "not-my-token", use_delex=use_delex) is False
    assert r.get("t:job").decode() == token


@BOTH
def test_release_with_own_token_deletes(r, use_delex):
    token = acquire(r, "t:job", ttl_ms=5000, wait_s=0)
    assert release(r, "t:job", token, use_delex=use_delex) is True
    assert r.exists("t:job") == 0


@BOTH
def test_lock_expires_and_late_release_spares_next_holder(r, use_delex):
    old = acquire(r, "t:job", ttl_ms=200, wait_s=0)
    time.sleep(0.3)
    new = acquire(r, "t:job", ttl_ms=5000, wait_s=0)  # the lease ran out, so B gets it
    assert new is not None
    assert release(r, "t:job", old, use_delex=use_delex) is False  # A's late release...
    assert r.get("t:job").decode() == new                          # ...leaves B's lock alone


def test_plain_del_would_remove_next_holders_lock(r):
    acquire(r, "t:job", ttl_ms=200, wait_s=0)
    time.sleep(0.3)
    acquire(r, "t:job", ttl_ms=5000, wait_s=0)
    r.delete("t:job")  # what a token-less release does
    assert r.exists("t:job") == 0  # B's lock is gone: a third worker can walk in


def test_set_ifeq_extends_only_our_own_lease(r):
    """Redis 8.4+: SET ... IFEQ is a one-command extend (no Lua needed)."""
    token = acquire(r, "t:job", ttl_ms=1000, wait_s=0)
    assert r.execute_command("SET", "t:job", token, "IFEQ", token, "PX", 9000) is True
    assert 8000 < r.pttl("t:job") <= 9000
    assert r.execute_command("SET", "t:job", "intruder", "IFEQ", "wrong", "PX", 60000) is None
    assert r.get("t:job").decode() == token and r.pttl("t:job") <= 9000


def test_setnx_then_expire_leaves_a_lock_without_ttl_if_you_crash_between(r):
    assert r.setnx("t:job", "x")
    # ...process dies here, before EXPIRE runs...
    assert r.pttl("t:job") == -1  # never expires: the job is blocked until someone deletes it
