"""The backend's half: whether a customer gets a refund is decided here, by rules, from
the booking record. The model never decides it and never words it.

The rules are the refund clauses (REF-01, REF-02, REF-04) written as code. A real system
would call the booking service; the point is that the answer is a function of the ticket.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    fare: str                    # Basic, Flex or Business
    price_cents: int             # everything the customer paid, taxes included
    taxes_cents: int             # the government taxes inside that price
    hours_since_purchase: float
    days_to_departure: float     # negative once the flight has left


@dataclass(frozen=True)
class Decision:
    eligible: bool
    amount_cents: int
    clause_id: str
    text: str                    # what the customer is told, word for word


def _money(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def eligibility(intent: str | None, ticket: Ticket | None) -> Decision | None:
    """A refund decision for this ticket, or None when the backend has no rule for it."""
    if intent != "refund" or ticket is None:
        return None
    t = ticket
    if t.days_to_departure < 0:
        return Decision(False, 0, "REF-01",
                        f"Ticket {t.ticket_id} can't be refunded: the flight has already "
                        "departed, and a fare is only refundable when it is canceled before "
                        "departure.")
    if t.hours_since_purchase <= 24 and t.days_to_departure >= 7:
        return Decision(True, t.price_cents, "REF-04",
                        f"Ticket {t.ticket_id} was bought less than 24 hours ago for a flight "
                        f"at least 7 days away, so it can be canceled for a full refund of "
                        f"{_money(t.price_cents)}.")
    if t.fare in ("Flex", "Business"):
        return Decision(True, t.price_cents, "REF-01",
                        f"Ticket {t.ticket_id} is a {t.fare} fare, so canceling it before "
                        f"departure refunds {_money(t.price_cents)} to the original form of "
                        "payment.")
    if t.fare == "Basic":
        return Decision(False, t.taxes_cents, "REF-02",
                        f"Ticket {t.ticket_id} is a Basic fare, which is non-refundable. If you "
                        f"cancel it, only the unused government taxes ({_money(t.taxes_cents)}) "
                        "are returned.")
    return None
