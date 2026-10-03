"""The policy store and retrieval, on PostgreSQL 18."""
import psycopg
import pytest

from bot import retrieve
from db import publish_clause


def test_the_bereavement_question_finds_the_bereavement_clause_first(conn):
    clauses = retrieve(conn, "Can I apply for the bereavement fare after I have already flown?")
    assert clauses[0].clause_id == "BRV-02"
    assert clauses[0].version == 1 and clauses[0].policy_type == "bereavement"
    assert 0 < clauses[0].score <= 1
    assert [c.score for c in clauses] == sorted((c.score for c in clauses), reverse=True)
    assert len(clauses) == 5


def test_the_score_is_the_share_of_the_question_s_words_in_the_clause(conn):
    # 'check', 'bag' and 'cost' are in BAG-02; 'much' is not.
    top = retrieve(conn, "How much does a checked bag cost?")[0]
    assert top.clause_id == "BAG-02" and top.score == pytest.approx(0.75)


def test_a_question_that_shares_no_word_with_the_policy_retrieves_nothing(conn):
    assert retrieve(conn, "Do you ship cargo?") == []
    assert retrieve(conn, "the and of") == []       # only stop words


def test_a_new_version_replaces_the_old_one_in_retrieval(conn):
    question = "What is the deadline to apply for a refund?"
    before = next(c for c in retrieve(conn, question) if c.clause_id == "REF-03")
    assert before.version == 1 and "90 days" in before.body

    version = publish_clause(conn, "REF-03", before.body.replace("90 days", "60 days"))

    assert version == 2
    after = [c for c in retrieve(conn, question) if c.clause_id == "REF-03"]
    assert len(after) == 1 and after[0].version == 2 and "60 days" in after[0].body


def test_the_old_version_stays_in_the_table_with_a_closed_range(conn):
    publish_clause(conn, "REF-03", "To request a refund, submit the form within 60 days.")
    rows = conn.execute(
        "SELECT version, upper_inf(effective), effective @> now() FROM policy_clause "
        "WHERE clause_id = 'REF-03' ORDER BY version").fetchall()
    assert rows == [(1, False, False), (2, True, True)]


def test_two_versions_of_a_clause_cannot_be_in_force_at_once(conn):
    with pytest.raises(psycopg.errors.ExclusionViolation):
        conn.execute(
            "INSERT INTO policy_clause (clause_id, version, policy_type, owner, body) "
            "VALUES ('BRV-02', 2, 'bereavement', 'Customer Relations', 'A second truth.')")


def test_what_did_the_policy_say_on_a_given_day(conn):
    """The audit question a dispute needs answered: the wording in force at a past moment."""
    (then,) = conn.execute("SELECT now()").fetchone()
    publish_clause(conn, "BRV-05", "Bereavement fares must be booked within 14 days of departure.")
    (body,) = conn.execute(
        "SELECT body FROM policy_clause WHERE clause_id = 'BRV-05' AND effective @> %s",
        (then,)).fetchone()
    assert "no more than 10 days" in body
