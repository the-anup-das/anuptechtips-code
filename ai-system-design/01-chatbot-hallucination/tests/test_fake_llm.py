"""The stand-in model: deterministic, and wrong in the ways it is asked to be."""
from collections import Counter

from bot import retrieve
from conftest import model
from fake_llm import RATES, FakeLLM
from questions import KEY, QUESTIONS

BEREAVEMENT = "Can I apply for the bereavement fare after I have already flown?"


def test_the_same_seed_and_question_give_the_same_draft(conn):
    clauses = retrieve(conn, BEREAVEMENT)
    assert model(seed=7).draft(BEREAVEMENT, clauses) == model(seed=7).draft(BEREAVEMENT, clauses)


def test_different_seeds_fail_on_different_questions(conn):
    clauses = {q.text: retrieve(conn, q.text) for q in QUESTIONS[:150]}
    modes = [[FakeLLM(seed, KEY).draft(t, c).mode for t, c in clauses.items()] for seed in (1, 2)]
    assert modes[0] != modes[1]


def test_the_rates_add_up_and_the_mix_is_roughly_what_they_say(conn):
    assert all(abs(sum(r.values()) - 1) < 1e-9 for r in RATES.values())
    answerable = [q for q in QUESTIONS if q.kind == "answerable"]
    seen = Counter(FakeLLM(seed, KEY).draft(q.text, retrieve(conn, q.text)).mode
                   for seed in (1, 2) for q in answerable)
    assert 0.6 < seen["faithful"] / 300 < 0.8
    assert seen["blend"] > 20 and seen["wrong_clause"] > 20


def test_faithful_quotes_the_right_clause_or_rewords_it_with_the_same_numbers(conn):
    question = "How much does a checked bag cost?"
    clauses = retrieve(conn, question)
    for seed in range(1, 9):
        draft = model("faithful", seed).draft(question, clauses)
        assert draft.cited == (("BAG-02", 1),)
        assert "$35" in draft.text and "$50" in draft.text and "23 kg" in draft.text


def test_blend_drops_the_negation_and_borrows_a_condition(conn):
    clauses = retrieve(conn, BEREAVEMENT)
    draft = model("blend").draft(BEREAVEMENT, clauses)
    assert draft.mode == "blend" and draft.cited == (("BRV-02", 1),)
    assert "cannot" not in draft.text and "can be applied to a ticket after travel" in draft.text


def test_wrong_clause_quotes_another_retrieved_clause_word_for_word(conn):
    clauses = retrieve(conn, BEREAVEMENT)
    draft = model("wrong_clause").draft(BEREAVEMENT, clauses)
    quoted = next(c for c in clauses if (c.clause_id, c.version) in draft.cited)
    assert quoted.clause_id != "BRV-02" and draft.text == quoted.body


def test_gap_fill_invents_a_rule_about_the_topic(conn):
    question = "Is there Wi-Fi on board?"
    draft = model().draft(question, retrieve(conn, question))
    assert draft.mode == "gap_fill" and "Wi-Fi on board" in draft.text


def test_with_nothing_retrieved_it_still_answers(conn):
    draft = model("faithful").draft("Do you ship cargo?", [])
    assert draft.mode == "gap_fill" and draft.cited == ()


def test_agree_repeats_the_customer_s_claim_and_adds_binding_when_asked(conn):
    question = next(q for q in QUESTIONS if q.binding)
    draft = model("agree").draft(question.text, retrieve(conn, question.text))
    assert draft.text == f"Yes, {question.claim}. That is a binding commitment."
