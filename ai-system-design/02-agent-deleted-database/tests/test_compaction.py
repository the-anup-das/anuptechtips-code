"""Compaction: a rule that lives in the conversation can be summarized away. The stand-in
then stops asking, and only something outside the conversation still says no."""
import pytest

import lab
from replay import SETUPS, World
from standin import RULE, Context, clean_inbox

EMAILS = range(1, 201)


def test_context_keeps_everything_below_its_limit():
    ctx = Context(limit=10)
    for i in range(10):
        ctx.add(f"message {i}")
    assert len(ctx.messages) == 10 and ctx.compactions == 0


def test_compaction_replaces_the_older_half_with_one_summary_line():
    ctx = Context(limit=10)
    ctx.add(f"user: {RULE}")
    for i in range(10):
        ctx.add(f"message {i}")
    assert ctx.compactions == 1
    assert ctx.messages[0] == "(summary: 5 earlier messages about cleaning the inbox)"
    assert ctx.messages[1:] == [f"message {i}" for i in range(4, 10)]
    assert not ctx.sees(RULE)


def test_with_room_for_the_whole_conversation_the_stand_in_asks_every_time():
    deleted = []
    result = clean_inbox(Context(limit=1000), EMAILS, ask=lambda _: False, delete=deleted.append)
    assert result == {"asked": 200, "unasked": 0, "first_unasked": None, "compactions": 0}
    assert deleted == []


def test_after_the_first_compaction_the_stand_in_stops_asking():
    deleted = []
    result = clean_inbox(Context(limit=50), EMAILS, ask=lambda _: False, delete=deleted.append)
    assert result["first_unasked"] == 18 and result["asked"] == 17
    assert result["unasked"] == 183 == len(deleted)
    assert deleted == list(range(18, 201))


def test_a_yes_is_still_a_yes_while_the_rule_is_visible():
    deleted = []
    clean_inbox(Context(limit=1000), EMAILS, ask=lambda i: i % 2 == 0, delete=deleted.append)
    assert deleted == list(range(2, 201, 2))


@pytest.fixture
def inbox():
    lab.reset(prod=("inbox",))


def run_cleanup(setup, r, tmp_path, limit):
    world = World(setup, r, tmp_path)
    try:
        result = clean_inbox(
            Context(limit), EMAILS, ask=lambda _: False,
            delete=lambda i: world.run("prod", f"DELETE FROM prod.inbox WHERE id = {i}"))
    finally:
        world.close()
    with lab.connect() as owner:
        left = owner.execute("SELECT count(*) FROM prod.inbox").fetchone()[0]
    return result, left, [attempt.error for attempt in world.attempts]


def test_without_a_gate_the_unasked_deletes_happen(inbox, r, tmp_path):
    result, left, errors = run_cleanup(SETUPS[0], r, tmp_path, limit=50)
    assert result["unasked"] == 183 and left == 17
    assert errors == [""] * 183


def test_behind_the_gate_no_unasked_delete_runs(inbox, r, tmp_path):
    result, left, errors = run_cleanup(SETUPS[2], r, tmp_path, limit=50)
    assert result["unasked"] == 183 and left == 200
    assert errors[:5] == ["gate: approval required"] * 5  # then the token bucket is empty
    assert errors[5:] == ["gate: rate limit"] * 178
