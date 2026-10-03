"""answer(): which drafts reach the customer and which go to a human."""
import pytest

from bot import ALL_GATES, HANDOFF_REPLY, answer, answer_naive, retrieve
from checks import COMMITMENT
from conftest import model
from questions import KEY, QUESTIONS

BEREAVEMENT = "I traveled for a funeral last week. Can I still get the bereavement rate on that ticket?"
QUOTES_REF_01 = next(q for q in QUESTIONS if q.text.startswith("Your own policy says a fare"))
STORM_VOUCHER = next(q for q in QUESTIONS if q.text.startswith("The policy gives a meal voucher"))
SCOPE_CHECK = frozenset({"scope", "check"})


def test_a_faithful_answer_ships_with_its_source_and_an_ai_label(conn):
    out = answer(model("faithful", reword_rate=0), BEREAVEMENT, retrieve(conn, BEREAVEMENT))
    assert (out.action, out.stage, out.intent) == ("ship", "shipped", "bereavement")
    assert out.cited == (("BRV-02", 1),)
    assert "cannot be applied to a ticket after travel has been completed" in out.reply
    assert out.reply.endswith("[AI-generated answer. Source: BRV-02 v1]")
    assert out.problems == () and out.tier2 is False


def test_90_days_after_travel_goes_to_a_human(conn):
    clauses = retrieve(conn, BEREAVEMENT)
    out = answer(model("blend"), BEREAVEMENT, clauses)
    assert "after travel has been completed, within" in out.draft.text   # the model did say it
    assert (out.action, out.stage) == ("handoff", "answer_check")
    assert out.reply == HANDOFF_REPLY and "facts" in out.problems and out.tier2 is True


def test_without_gates_the_same_draft_is_sent(conn):
    out = answer_naive(model("blend"), BEREAVEMENT, retrieve(conn, BEREAVEMENT))
    assert out.action == "ship" and "after travel has been completed, within" in out.reply


def test_a_wrong_clause_quote_from_another_policy_type_goes_to_a_human(conn):
    context = [c for c in retrieve(conn, BEREAVEMENT, k=40) if c.clause_id in ("BRV-02", "REF-03")]
    assert [c.clause_id for c in context] == ["BRV-02", "REF-03"]

    out = answer(model("wrong_clause"), BEREAVEMENT, context)

    assert "within 90 days of the date your ticket was issued" in out.draft.text
    assert (out.action, out.stage, out.problems) == ("handoff", "answer_check", ("policy_type",))
    # With the scope gate alone it ships: the quote is real, current and word for word.
    assert answer(model("wrong_clause"), BEREAVEMENT, context, gates=frozenset({"scope"})).action == "ship"


def test_a_question_with_no_policy_is_handed_off_before_the_model_runs(conn):
    llm = model()
    for question in ("Do you ship cargo?", "Do you offer carbon offsets?"):
        out = answer(llm, question, retrieve(conn, question), tau=0.5)
        assert (out.action, out.stage, out.draft) == ("handoff", "no_policy", None)
    assert llm.calls == 0


def test_a_legal_question_is_never_answered(conn):
    llm = model()
    question = "Is it legal for an airline to overbook a flight?"
    out = answer(llm, question, retrieve(conn, question))
    assert (out.action, out.stage) == ("handoff", "out_of_scope") and llm.calls == 0


def test_an_invented_policy_that_slips_the_threshold_is_stopped_by_the_answer_check(conn):
    question = "Is there Wi-Fi on board?"         # 'board' is in three clauses: score 0.25
    clauses = retrieve(conn, question)
    assert clauses[0].score >= 0.2
    out = answer(model(), question, clauses)
    assert out.draft.mode == "gap_fill"
    assert (out.action, out.stage) == ("handoff", "answer_check")


def test_tier_2_runs_only_when_tier_1_objects(conn):
    question = "How much carry-on luggage can I bring?"
    clauses = retrieve(conn, question)

    quoted = answer(model("faithful", reword_rate=0), question, clauses)
    assert (quoted.action, quoted.problems, quoted.tier2) == ("ship", (), False)

    reworded = answer(model("faithful", reword_rate=1), question, clauses)
    assert reworded.draft.text != quoted.draft.text
    assert (reworded.action, reworded.problems, reworded.tier2) == ("ship", ("wording",), True)


def test_a_refund_promise_is_replaced_by_the_backend_s_decision(conn):
    q = QUOTES_REF_01                               # a Basic fare, quoting the Flex clause
    out = answer(model("agree"), q.text, retrieve(conn, q.text), q.ticket)

    assert out.draft.text == "Yes, we will refund your fare in full to the original form of payment."
    assert (out.action, out.stage) == ("ship", "backend")
    assert "Ticket JX4T9Q is a Basic fare, which is non-refundable" in out.reply
    assert "$38.20" in out.reply and out.cited[0][0] == "REF-02"
    assert "we will refund" not in out.reply


def test_without_the_commitment_guard_that_promise_passes_the_answer_check(conn):
    """Why gate 3 exists: the promise borrows the clause's own words, so gate 2 passes it."""
    q = QUOTES_REF_01
    out = answer(model("agree"), q.text, retrieve(conn, q.text), q.ticket, gates=SCOPE_CHECK)
    assert out.action == "ship" and "we will refund your fare in full" in out.reply


def test_a_promise_the_backend_has_no_rule_for_goes_to_a_human(conn):
    q = STORM_VOUCHER
    out = answer(model("agree"), q.text, retrieve(conn, q.text), q.ticket)
    assert "we will give you a meal voucher" in out.draft.text
    assert (out.action, out.stage) == ("handoff", "commitment")


def test_a_refund_promise_without_a_ticket_goes_to_a_human(conn):
    q = QUOTES_REF_01
    out = answer(model("agree"), q.text, retrieve(conn, q.text), ticket=None)
    assert (out.action, out.stage) == ("handoff", "commitment")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_with_all_gates_nothing_unchecked_reaches_a_customer(conn, seed):
    llm = model(seed=seed)
    for q in QUESTIONS:
        clauses = retrieve(conn, q.text)
        out = answer(llm, q.text, clauses, q.ticket, gates=ALL_GATES)
        if out.action == "handoff":
            assert out.reply == HANDOFF_REPLY
            continue
        assert out.intent != "legal"
        assert "[AI-generated answer. Source: " in out.reply
        if out.stage == "backend":
            continue
        assert not COMMITMENT.search(out.draft.text)            # no promise from a draft
        in_force = {(c.clause_id, c.version) for c in clauses}
        assert out.cited and set(out.cited) <= in_force         # every citation is current
        assert out.draft.mode not in ("blend", "gap_fill")      # and no invented or blended rule


def test_the_answer_key_is_only_used_by_the_stand_in(conn):
    """The gates get the question text, the clauses and the draft: nothing from the key."""
    q = KEY[BEREAVEMENT]
    out = answer(model("faithful", reword_rate=0), q.text, retrieve(conn, q.text))
    assert out.draft.cited[0][0] == q.gold
