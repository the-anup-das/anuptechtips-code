"""handle(): the kill switch, the versioned cache and the audit row (Postgres + Redis)."""
import psycopg
import pytest
import redis

from bot import HANDOFF_REPLY
from conftest import model
from db import publish_clause
from questions import QUESTIONS
from service import KILL_SWITCH, cache_key, cache_key_naive, handle, record

DEADLINE = "What is the deadline to apply for a refund?"
QUOTES_REF_01 = next(q for q in QUESTIONS if q.text.startswith("Your own policy says a fare"))


def faithful():
    return model("faithful", reword_rate=0)


def audits(conn):
    return conn.execute("SELECT conversation_id, question, action, stage, draft, reply, cited, "
                        "checks FROM answer_audit ORDER BY id").fetchall()


def test_every_reply_leaves_an_audit_row_and_an_outbox_event(conn, r):
    out = handle(conn, r, faithful(), "conv-1", DEADLINE)

    (row,) = audits(conn)
    assert row[:4] == ("conv-1", DEADLINE, "ship", "shipped")
    assert "within 90 days" in row[4] and row[5] == out.reply
    assert row[6] == [["REF-03", 1]]                 # the clause version behind the answer
    assert row[7] == {"problems": [], "tier2": False, "cached": False}

    (topic, key, payload, published) = conn.execute(
        "SELECT topic, key, payload, published_at FROM outbox").fetchone()
    assert (topic, key, published) == ("chatbot.audit", "conv-1", None)
    assert payload["reply"] == out.reply and payload["cited"] == [["REF-03", 1]]


def test_a_hand_off_is_audited_with_the_draft_the_customer_never_saw(conn, r):
    question = "Can I apply for the bereavement fare after I have already flown?"
    out = handle(conn, r, model("blend"), "conv-2", question)

    (row,) = audits(conn)
    assert out.action == "handoff" and row[2:4] == ("handoff", "answer_check")
    assert "can be applied to a ticket after travel" in row[4]     # the draft
    assert row[5] == HANDOFF_REPLY                                 # the customer's copy
    assert row[7]["problems"] and row[7]["tier2"] is True


def test_the_audit_row_and_its_event_commit_together_or_not_at_all(conn, r):
    out = handle(conn, r, faithful(), "conv-3", DEADLINE)
    with pytest.raises(psycopg.errors.NotNullViolation):
        record(conn, "conv-3", DEADLINE, out, topic=None)          # the outbox insert fails
    assert conn.execute("SELECT count(*) FROM answer_audit").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1


def test_a_repeated_question_is_served_from_the_cache_and_still_audited(conn, r):
    llm = faithful()
    first = handle(conn, r, llm, "conv-4", DEADLINE)
    second = handle(conn, r, llm, "conv-5", "  what is the DEADLINE to apply for a refund? ")

    assert llm.calls == 1 and second.cached and second.reply == first.reply
    rows = audits(conn)
    assert len(rows) == 2 and rows[1][7]["cached"] is True and rows[1][6] == [["REF-03", 1]]


def test_no_stale_answer_after_a_clause_is_republished(conn, r):
    llm = faithful()
    before = handle(conn, r, llm, "conv-6", DEADLINE)
    assert "within 90 days" in before.reply and before.cited == (("REF-03", 1),)

    publish_clause(conn, "REF-03", "To request a refund for an unused ticket, submit the Ticket "
                   "Refund Application form within 60 days of the date your ticket was issued.")
    after = handle(conn, r, llm, "conv-7", DEADLINE)

    assert not after.cached and llm.calls == 2
    assert "within 60 days" in after.reply and after.cited == (("REF-03", 2),)


def test_a_key_without_the_version_keeps_quoting_the_old_clause(conn, r):
    """The bug the versioned key prevents."""
    llm = faithful()
    handle(conn, r, llm, "conv-8", DEADLINE, key_for=cache_key_naive)
    publish_clause(conn, "REF-03", "To request a refund for an unused ticket, submit the Ticket "
                   "Refund Application form within 60 days of the date your ticket was issued.")
    stale = handle(conn, r, llm, "conv-9", DEADLINE, key_for=cache_key_naive)

    assert stale.cached and "within 90 days" in stale.reply and stale.cited == (("REF-03", 1),)


def test_the_key_changes_when_any_clause_in_the_context_changes(conn):
    from bot import retrieve
    before = retrieve(conn, DEADLINE)
    publish_clause(conn, before[-1].clause_id, before[-1].body + " Ask at the desk.")
    after = retrieve(conn, DEADLINE)
    assert cache_key(DEADLINE, before) != cache_key(DEADLINE, after)
    assert cache_key_naive(DEADLINE, before) == cache_key_naive(DEADLINE, after)


def test_hand_offs_and_backend_decisions_are_not_cached(conn, r):
    q = QUOTES_REF_01
    handle(conn, r, model("agree"), "conv-10", q.text, q.ticket)        # backend decision
    handle(conn, r, model("blend"), "conv-11", DEADLINE)                # hand-off
    assert r.keys("chatbot:answer:*") == []


def test_the_kill_switch_sends_everything_to_a_human(conn, r):
    llm = faithful()
    r.set(KILL_SWITCH, 1)
    out = handle(conn, r, llm, "conv-12", DEADLINE)
    assert (out.action, out.stage, out.reply) == ("handoff", "kill_switch", HANDOFF_REPLY)
    assert llm.calls == 0 and audits(conn)[0][3] == "kill_switch"

    r.delete(KILL_SWITCH)
    assert handle(conn, r, llm, "conv-12", DEADLINE).action == "ship"


def test_if_the_switch_cannot_be_read_the_bot_fails_closed(conn):
    dead = redis.Redis(host="localhost", port=1, socket_connect_timeout=0.2)
    llm = faithful()
    out = handle(conn, dead, llm, "conv-13", DEADLINE)
    assert (out.action, out.stage) == ("handoff", "kill_switch") and llm.calls == 0
    assert len(audits(conn)) == 1                   # and it is still audited
