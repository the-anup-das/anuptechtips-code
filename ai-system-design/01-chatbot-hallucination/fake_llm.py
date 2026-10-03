"""A deterministic stand-in for the model. No API key, no weights, no network.

It fails on purpose, in the three shapes the real cases showed, so the gates have
something to catch. For every question it rolls a seeded die and writes one of:

  faithful      the right clause, word for word or lightly reworded with small talk
  blend         the right clause with its "not" removed and a condition from a second
                clause spliced in (Air Canada: "... after travel, within 90 days ...")
  wrong_clause  another retrieved clause, quoted word for word
  gap_fill      a rule that exists nowhere, in policy language (Cursor)
  agree         "Yes, <whatever the customer asserted>", plus "binding" if they asked

The stand-in holds the answer key (questions.KEY), which is how it knows which clause is
the right one and what kind of question it got. The gates never see the key or the mode.
"""
import random
import re

from checks import FACT, NEGATION, facts, overlap
from models import Clause, Draft
from questions import Question

# How often each kind of question gets each kind of draft.
RATES = {
    "answerable": {"faithful": 0.70, "blend": 0.15, "wrong_clause": 0.15},
    "leading": {"agree": 0.60, "faithful": 0.40},
    "no_policy": {"gap_fill": 1.0},
    "legal": {"gap_fill": 1.0},
}
REWORD_RATE = 0.5       # share of faithful drafts that are reworded instead of quoted

OPENERS = ["Thanks for reaching out.", "Happy to help with that.", "Good question.",
           "Thank you for your message."]
CLOSERS = ["Let me know if you need anything else.", "I hope this helps.",
           "Is there anything else I can help you with?", "Feel free to ask if anything is unclear."]
REWORDINGS = [
    ("before departure", "before the flight leaves"), ("free of charge", "at no charge"),
    ("must", "will need to"), ("may", "can"), ("passengers", "travelers"),
    ("passenger", "traveler"), ("costs", "is priced at"), ("provides", "gives you"),
    ("purchase", "buying it"), ("at least", "a minimum of"), ("fee", "charge"),
    ("submit", "send in"), ("is refunded", "is paid back"), ("are returned", "are given back"),
    ("requires", "needs"), ("canceled", "called off"), ("requested", "asked for"),
]
NO_POLICY_TEMPLATES = [
    "Juniper Air's policy on {topic} is simple: it is included with every ticket at no extra "
    "charge.",
    "Under Juniper Air's policy, {topic} costs ${n} and can be arranged at the airport.",
    "Juniper Air's policy on {topic} requires a request at least {h} hours before departure.",
    "Juniper Air does not allow {topic}. This is a core part of our security policy.",
]
LEGAL_TEMPLATES = [
    "Yes, that is legal. Airlines are free to set their own rules on {topic}.",
    "No, that is not legal. The law entitles you to ${n} in compensation for {topic}.",
    "Yes, the law is on your side. The rules on {topic} always favor the passenger.",
]
FLIPS = [("cannot", "can"), ("are not", "are"), ("is not", "is"), ("did not", "did"),
         ("non-refundable", "refundable"), ("never", "always"), ("not ", "")]
TAIL = re.compile(r"\b(?:within|up to|at least|no more than|for a|after)\s[^.]*?\d[^.]*"
                  r"|\$\d[^.]*|\d[^.]*")


class FakeLLM:
    def __init__(self, seed: int, key: dict[str, Question] | None = None,
                 rates: dict = RATES, force: str | None = None,
                 reword_rate: float = REWORD_RATE):
        self.seed, self.key, self.rates = seed, key or {}, rates
        self.force, self.reword_rate = force, reword_rate
        self.calls = 0

    def draft(self, question: str, clauses: list[Clause]) -> Draft:
        self.calls += 1
        q = self.key.get(question) or Question(question, "answerable")
        rng = random.Random(f"{self.seed}|{question}")    # same seed, same question, same draft
        modes = self.rates[q.kind]
        mode = self.force or rng.choices(list(modes), weights=list(modes.values()))[0]
        if not clauses:
            return self._gap_fill(q, clauses, rng)          # nothing to quote: make it up
        right = next((c for c in clauses if c.clause_id == q.gold), clauses[0])
        others = [c for c in clauses if c is not right]

        if mode == "gap_fill":
            return self._gap_fill(q, clauses, rng)
        if mode == "agree":
            text = f"Yes, {q.claim or 'that is correct'}."
            # It cites whichever retrieved clause sounds most like what it just agreed to.
            support = max(clauses, key=lambda c: overlap(text, c.body))
            if q.binding:
                text += " That is a binding commitment."
            return Draft(text, _cite(support), "agree")
        if mode == "wrong_clause" and others:
            wrong = rng.choice(others)
            return Draft(wrong.body, _cite(wrong), "wrong_clause")
        if mode in ("blend", "wrong_clause"):               # wrong_clause with nothing else to quote
            blended = _blend(right, others)
            if blended:
                return Draft(blended, _cite(right), "blend")
        return self._faithful(right, rng)

    def _faithful(self, clause: Clause, rng: random.Random) -> Draft:
        if rng.random() >= self.reword_rate:
            return Draft(clause.body, _cite(clause), "faithful")
        text = clause.body
        for old, new in REWORDINGS:
            if rng.random() < 0.5:
                text = re.sub(rf"\b{old}\b", new, text)
        text = f"{rng.choice(OPENERS)} {text} {rng.choice(CLOSERS)}"
        return Draft(text, _cite(clause), "faithful")

    def _gap_fill(self, q: Question, clauses: list[Clause], rng: random.Random) -> Draft:
        templates = LEGAL_TEMPLATES if q.kind == "legal" else NO_POLICY_TEMPLATES
        text = rng.choice(templates).format(topic=q.topic or "that", n=rng.choice([20, 45, 60]),
                                            h=rng.choice([24, 48, 72]))
        text = text[0].upper() + text[1:]
        # Half the time it cites the nearest clause anyway, which makes the answer look sourced.
        cited = _cite(clauses[0]) if clauses and rng.random() < 0.5 else ()
        return Draft(text, cited, "gap_fill")


def _cite(clause: Clause) -> tuple[tuple[str, int], ...]:
    return ((clause.clause_id, clause.version),)


def _blend(right: Clause, others: list[Clause]) -> str | None:
    """The right clause, minus its negation, plus a condition lifted from another clause."""
    text = right.body
    if NEGATION.search(text.lower()):
        for old, new in FLIPS:
            if old in text:
                text = text.replace(old, new, 1)
                break
    for other in others:
        tail = TAIL.search(other.body)
        if tail and not facts(tail.group()) <= facts(right.body):
            text = f"{text.rstrip('.')}, {_lower_first(tail.group())}."
            break
    return text if text != right.body else None


def _lower_first(s: str) -> str:
    return s if FACT.match(s) else s[0].lower() + s[1:]
