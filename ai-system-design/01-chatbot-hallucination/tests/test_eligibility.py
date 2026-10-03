"""The backend's refund rules."""
from eligibility import Ticket, eligibility


def ticket(fare="Basic", hours=240.0, days=12.0) -> Ticket:
    return Ticket("AB12CD", fare, 31900, 3820, hours_since_purchase=hours, days_to_departure=days)


def test_a_basic_fare_gets_only_its_taxes_back():
    d = eligibility("refund", ticket("Basic"))
    assert (d.eligible, d.amount_cents, d.clause_id) == (False, 3820, "REF-02")
    assert "non-refundable" in d.text and "$38.20" in d.text


def test_flex_and_business_fares_are_refunded_in_full_before_departure():
    for fare in ("Flex", "Business"):
        d = eligibility("refund", ticket(fare))
        assert (d.eligible, d.amount_cents, d.clause_id) == (True, 31900, "REF-01")
        assert "$319.00" in d.text


def test_any_fare_is_refunded_inside_the_24_hour_window():
    d = eligibility("refund", ticket("Basic", hours=3, days=30))
    assert (d.eligible, d.amount_cents, d.clause_id) == (True, 31900, "REF-04")


def test_the_24_hour_window_needs_a_flight_at_least_7_days_away():
    d = eligibility("refund", ticket("Basic", hours=3, days=2))
    assert (d.eligible, d.clause_id) == (False, "REF-02")


def test_nothing_is_refunded_after_departure():
    d = eligibility("refund", ticket("Flex", days=-1))
    assert (d.eligible, d.amount_cents) == (False, 0)


def test_no_ticket_no_rule_or_another_intent_means_no_decision():
    assert eligibility("refund", None) is None
    assert eligibility("refund", ticket("Standard")) is None     # no rule for this fare
    assert eligibility("disruption", ticket("Flex")) is None
    assert eligibility(None, ticket("Flex")) is None
