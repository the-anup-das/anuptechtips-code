"""The keyword intent router."""
import pytest

from bot import OUT_OF_SCOPE, route
from corpus import POLICY_TYPES


@pytest.mark.parametrize("question, intent", [
    ("Can I apply for the bereavement fare after I have already flown?", "bereavement"),
    ("My stepbrother passed away. Would I qualify?", "bereavement"),
    ("Are Basic fares refundable?", "refund"),
    ("How heavy can a checked bag be?", "baggage"),
    ("Is there a change fee on Flex fares?", "changes"),
    ("My flight is delayed four hours. Do I get a meal?", "disruption"),
    ("When does online check-in open?", "checkin"),
    ("Can my cat fly in the cabin with me?", "special"),
    ("Do Juniper Points expire?", "loyalty"),
])
def test_a_question_is_routed_to_its_policy_type(question, intent):
    assert route(question) == intent
    assert intent in POLICY_TYPES


@pytest.mark.parametrize("question", [
    "Is it legal for an airline to overbook a flight?",
    "Can I sue Juniper Air for a missed connection?",
    "What are my passenger rights if my flight is canceled?",
    "Am I legally entitled to compensation for a three-hour delay?",
])
def test_a_question_about_the_law_is_out_of_scope_whatever_else_it_mentions(question):
    assert route(question) == OUT_OF_SCOPE


def test_a_longer_phrase_outweighs_a_single_word():
    # "denied boarding" (disruption) beats "boarding" (check-in)
    assert route("How much compensation is paid for denied boarding?") == "disruption"


def test_no_keyword_or_a_tie_gives_no_intent():
    assert route("Do you offer carbon offsets?") is None
    assert route("Can I transfer points to my wife?") is None     # changes or loyalty?


def test_a_keyword_inside_a_hyphenated_word_does_not_count():
    # The first version sent this to the legal queue: "law" matched inside "mother-in-law".
    assert route("Is a mother-in-law considered immediate family for bereavement travel?") \
        == "bereavement"
