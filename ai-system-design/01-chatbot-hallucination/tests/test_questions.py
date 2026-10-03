"""The question set and its answer key."""
from collections import Counter

from corpus import CLAUSES, POLICY_TYPES
from questions import KEY, QUESTIONS, TICKETS


def test_300_questions_in_the_four_groups():
    assert Counter(q.kind for q in QUESTIONS) == {
        "answerable": 150, "no_policy": 50, "legal": 50, "leading": 50}
    assert len(KEY) == 300                          # no duplicates


def test_the_corpus_has_40_clauses_in_8_types_of_5():
    assert len(CLAUSES) == 40 and len({c.clause_id for c in CLAUSES}) == 40
    assert Counter(c.policy_type for c in CLAUSES) == dict.fromkeys(POLICY_TYPES, 5)


def test_every_gold_clause_exists_and_every_clause_is_asked_about():
    ids = {c.clause_id for c in CLAUSES}
    assert {q.gold for q in QUESTIONS if q.gold} <= ids
    assert {q.gold for q in QUESTIONS if q.kind == "answerable"} == ids
    assert all(q.gold is None for q in QUESTIONS if q.kind in ("no_policy", "legal"))


def test_leading_questions_carry_a_claim_and_the_others_a_topic():
    assert all(q.claim and q.gold for q in QUESTIONS if q.kind == "leading")
    assert all(q.topic for q in QUESTIONS if q.kind in ("no_policy", "legal"))


def test_every_ticket_mentioned_in_a_question_is_known_to_the_backend():
    with_ticket = [q for q in QUESTIONS if q.ticket]
    assert {q.ticket.ticket_id for q in with_ticket} == set(TICKETS)
    assert all(q.kind == "leading" for q in with_ticket)
