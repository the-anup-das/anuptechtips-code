"""Tier 1, the judge stand-in and the commitment pattern. Pure Python: no services needed."""
import pytest

from checks import COMMITMENT, facts, judge, negations, overlap, stems, tier1_check
from corpus import CLAUSES
from fake_llm import _blend
from models import Draft


def cite(c):
    return ((c.clause_id, c.version),)


def test_facts_keeps_each_number_with_its_unit():
    assert facts("submit the form within 90 days of the date") == {"90 day"}
    assert facts("the 24-hour window, or 24 hours") == {"24 hour"}
    assert facts("costs $35 and the second costs $50. Up to 23 kg.") == {"$35", "$50", "23 kg"}
    assert facts("compensation is $1,000") == {"$1000"}
    assert facts("25,000 status points in a year") == {"25000 status point"}
    assert facts("aged 5 to 11") == {"5", "11"}
    assert facts("no numbers here") == set()


def test_negations_counts_not_cannot_never_and_contractions():
    assert negations("The policy cannot be applied after travel.") == 1
    assert negations("Basic fares are non-refundable.") == 1
    assert negations("Refunds are never paid in cash, and bags can't be late.") == 2
    assert negations("The ticket is treated as a no-show at no extra cost.") == 0


def test_stems_match_the_usual_word_forms():
    assert stems("refunded refunds refundable") == {"refund"}
    assert stems("canceled cancels cancel") == {"cancel"}
    assert stems("fares fare") == {"far"}
    assert stems("the airline's passengers") == {"airlin", "passenger"}
    assert overlap("a full refund", "Refunds are paid in full.") == 1.0


def test_a_word_for_word_quote_passes_tier_1(clause):
    brv = clause("BRV-02")
    assert tier1_check(Draft(brv.body, cite(brv)), [brv], "bereavement") == []


def test_90_days_after_travel_is_blocked(clause):
    """The Air Canada answer: the bereavement clause, minus its "cannot", plus the refund
    form's deadline."""
    brv, ref = clause("BRV-02"), clause("REF-03")
    text = _blend(brv, [ref])
    assert "can be applied to a ticket after travel has been completed, within 90 days" in text
    draft = Draft(text, cite(brv))

    problems = tier1_check(draft, [brv, ref], "bereavement")
    assert "facts" in problems                      # the clause it cites has no "90 days"
    assert not judge(draft, [brv, ref], "bereavement").grounded


def test_citing_both_clauses_does_not_rescue_the_blend(clause):
    brv, ref = clause("BRV-02"), clause("REF-03")
    draft = Draft(_blend(brv, [ref]), cite(brv) + cite(ref))
    assert tier1_check(draft, [brv, ref], "bereavement") != []
    assert not judge(draft, [brv, ref], "bereavement").grounded


def test_a_wrong_clause_quote_passes_everything_except_the_type_check(clause):
    """A real, current clause, quoted word for word: only its policy type gives it away."""
    brv, ref = clause("BRV-02"), clause("REF-03")
    draft = Draft(ref.body, cite(ref))              # the refund form, for a bereavement question

    assert tier1_check(draft, [brv, ref], "bereavement") == ["policy_type"]
    assert not judge(draft, [brv, ref], "bereavement").grounded
    assert tier1_check(draft, [brv, ref], "refund") == []   # citation checks alone: it ships


def test_a_wrong_clause_of_the_same_type_gets_through(clause):
    """The limit of tier 1: REF-01 quoted for a Basic-fare question is a correct quote of the
    wrong clause, and nothing here can tell."""
    ref1, ref2 = clause("REF-01"), clause("REF-02")
    assert tier1_check(Draft(ref1.body, cite(ref1)), [ref2, ref1], "refund") == []


@pytest.mark.parametrize("cited", [(), (("BRV-02", 7),), (("XXX-99", 1),)])
def test_no_citation_or_one_that_is_not_in_force_fails(clause, cited):
    brv = clause("BRV-02")
    draft = Draft(brv.body, cited)
    assert tier1_check(draft, [brv], "bereavement") == ["citation"]
    assert not judge(draft, [brv], "bereavement").grounded


def test_an_unknown_intent_fails_the_type_check(clause):
    brv = clause("BRV-02")
    assert tier1_check(Draft(brv.body, cite(brv)), [brv], None) == ["policy_type"]


def test_small_talk_fails_tier_1_and_the_judge_rescues_it(clause):
    bag = clause("BAG-01")
    text = f"Thanks for reaching out. {bag.body} Let me know if you need anything else."
    draft = Draft(text, cite(bag))

    assert tier1_check(draft, [bag], "baggage") == ["wording"]
    assert judge(draft, [bag], "baggage").grounded


def test_light_rewording_is_rescued_but_a_changed_number_is_not(clause):
    chk = clause("CHK-01")
    reworded = ("You can check in online from 24 hours before departure until 60 minutes "
                "before departure.")
    assert tier1_check(Draft(reworded, cite(chk)), [chk], "checkin") == ["wording"]
    assert judge(Draft(reworded, cite(chk)), [chk], "checkin").grounded

    wrong = reworded.replace("60 minutes", "30 minutes")
    assert not judge(Draft(wrong, cite(chk)), [chk], "checkin").grounded


def test_a_dropped_number_is_caught(clause):
    """'The first checked bag is free' reuses the clause's words and drops its $35."""
    bag = clause("BAG-02")
    draft = Draft("Yes, the first checked bag is free on a Basic fare.", cite(bag))
    assert tier1_check(draft, [bag], "baggage") == ["facts"]
    assert not judge(draft, [bag], "baggage").grounded


def test_an_invented_rule_is_not_supported_by_the_clause_it_cites(clause):
    chk = clause("CHK-03")
    draft = Draft("Juniper Air's policy on Wi-Fi on board is simple: it is included with every "
                  "ticket at no extra charge.", cite(chk))
    assert "wording" in tier1_check(draft, [chk], "checkin")
    assert not judge(draft, [chk], "checkin").grounded


def test_an_answer_that_is_only_small_talk_is_not_grounded(clause):
    bag = clause("BAG-01")
    assert not judge(Draft("Happy to help with that.", cite(bag)), [bag], "baggage").grounded


def test_known_gap_a_duration_written_in_words_is_invisible(clause):
    """The fact check reads digits. 'A full year' has none, so against a sentence without
    numbers this claim passes the judge. Found by the measurement, kept as a reminder."""
    ref = clause("REF-01")
    draft = Draft("Yes, you have a full year to send the refund form.", cite(ref))
    assert tier1_check(draft, [ref], "refund") == ["wording"]
    assert judge(draft, [ref], "refund").grounded


@pytest.mark.parametrize("text", [
    "Yes, we will refund your ticket in full.",
    "You are eligible for a full refund.",
    "You're entitled to $400.",
    "That is a binding commitment.",
    "I can confirm the fee is waived.",
    "We'll hold the gate, guaranteed.",
    "That's a deal.",
])
def test_commitment_wording_is_recognised(text):
    assert COMMITMENT.search(text)


def test_no_clause_reads_like_a_commitment():
    """A faithful quote of the policy must never trip the commitment guard."""
    assert [c.clause_id for c in CLAUSES if COMMITMENT.search(c.body)] == []
