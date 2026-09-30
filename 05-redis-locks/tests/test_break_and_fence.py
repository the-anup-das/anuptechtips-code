import time

import pytest
from redis.crc import key_slot

import break_it
import fence_it
from conftest import TEST_ROW
from fencing import FencedLock, claim_then_write, fenced_write, unfenced_write


def test_break_it_loses_an_update_every_time():
    """Deterministic: 20 runs, 20 lost updates (2 increments, final value 1)."""
    assert [break_it.run() for _ in range(20)] == [1] * 20


def test_fenced_pause_rejects_the_stale_write_and_ends_at_2():
    final, log = fence_it.run("pause", "write-only")
    assert final == 2
    assert log[:2] == ["B: token 34 accepted", "A: token 33 REJECTED"]
    assert "A: token 35 accepted" in log


def test_overlap_write_only_fencing_loses_one_update():
    final, log = fence_it.run("overlap", "write-only")
    assert final == 1  # A (33) wrote inside B's read-write gap, then B (34) overwrote it
    assert "A: token 33 accepted" in log and "B: token 34 accepted" in log


def test_overlap_claim_first_loses_nothing():
    final, log = fence_it.run("overlap", "claim-first")
    assert final == 2
    assert "A: token 33 REJECTED" in log


def test_fenced_lock_tokens_only_grow_and_it_still_excludes(r):
    tokens = []
    for _ in range(5):
        lock = FencedLock(r, "{t:job}", timeout=5)
        assert lock.acquire(blocking=False)
        assert not FencedLock(r, "{t:job}", timeout=5).acquire(blocking=False)
        tokens.append(lock.fence)
        lock.release()
    assert tokens == sorted(tokens) and len(set(tokens)) == 5


def test_fenced_lock_keys_share_a_cluster_slot(r):
    lock = FencedLock(r, "{t:job}", timeout=5)
    assert key_slot(lock.name.encode()) == key_slot(lock.fence_key.encode())


def test_fenced_lock_expired_release_raises(r):
    from redis.exceptions import LockNotOwnedError
    lock = FencedLock(r, "{t:job}", timeout=0.2)
    assert lock.acquire()
    time.sleep(0.3)
    with pytest.raises(LockNotOwnedError):
        lock.release()


def test_fenced_write_rejects_older_token_and_keeps_newer_value(db):
    assert fenced_write(db, TEST_ROW, 34, lambda v: v + 1) is True
    assert fenced_write(db, TEST_ROW, 33, lambda v: v + 100) is False  # stale: rowcount 0
    assert db.execute("SELECT value, fence FROM counters WHERE id = %s",
                      (TEST_ROW,)).fetchone() == (1, 34)


def test_fenced_write_strict_less_than_blocks_a_second_write_with_the_same_token(db):
    assert fenced_write(db, TEST_ROW, 34, lambda v: v + 1) is True
    assert fenced_write(db, TEST_ROW, 34, lambda v: v + 1) is False  # use <= if a lease writes twice


def test_claim_then_write_rejects_when_a_newer_token_claims_mid_work(db, db2):
    def slow_update(value: int) -> int:
        # while "A" (33) works, "B" (34) claims the row on another connection
        assert claim_then_write(db2, TEST_ROW, 34, lambda v: v + 1) is True
        return value + 1

    assert claim_then_write(db, TEST_ROW, 33, slow_update) is False
    assert db.execute("SELECT value, fence FROM counters WHERE id = %s",
                      (TEST_ROW,)).fetchone() == (1, 34)
    assert claim_then_write(db, TEST_ROW, 33, lambda v: v + 1) is False  # stale before it starts


def test_unfenced_write_accepts_anything(db):
    assert unfenced_write(db, TEST_ROW, lambda v: v + 1) is True
    assert unfenced_write(db, TEST_ROW, lambda v: 0) is True  # a stale writer can reset the row
