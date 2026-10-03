"""A support bot whose answers have to clear gates before a customer sees them:
retrieve the clauses in force, abstain when none fits, check the draft against the
clause it cites, and send anything that fails to a human.
"""
import re

import psycopg

from checks import COMMITMENT, judge, tier1_check
from eligibility import Ticket, eligibility
from models import Clause, Draft, Outcome

TAU = 0.2           # abstain when the best clause has fewer than this share of the question's words
K = 5               # clauses handed to the model
ALL_GATES = frozenset({"scope", "commitment", "check"})
HANDOFF_REPLY = ("I can't confirm that from our policy, so I'm passing this conversation to "
                 "a colleague who can.")

RETRIEVE = """
WITH q AS (
    SELECT tsvector_to_array(to_tsvector('english', %(question)s)) AS words,
           CAST(replace(plainto_tsquery('english', %(question)s)::text, '&', '|') AS tsquery)
               AS any_word
)
SELECT clause_id, version, policy_type, body,
       -- score: the share of the question's words that this clause contains
       cardinality(ARRAY(SELECT unnest(words) INTERSECT SELECT unnest(tsvector_to_array(tsv))))
           / greatest(cardinality(words), 1)::float8 AS score
FROM policy_clause, q
WHERE effective @> now()        -- only the version that is in force right now
  AND tsv @@ any_word           -- the GIN index finds clauses that share any word
ORDER BY score DESC, ts_rank_cd(tsv, any_word) DESC, clause_id
LIMIT %(k)s
"""


def retrieve(conn: psycopg.Connection, question: str, k: int = K) -> list[Clause]:
    rows = conn.execute(RETRIEVE, {"question": question, "k": k}).fetchall()
    return [Clause(*row) for row in rows]


# The intent router: which part of the policy is this question about? A keyword table is
# enough to show the idea; in production this is a small classifier.
OUT_OF_SCOPE = "legal"          # questions about the law go to a human, always
ROUTES = {
    "legal": r"legal\w*|illegal|laws?|lawful|sue|court|rights|regulations?|liable",
    "bereavement": r"bereavement|funeral|deceased|death|died|dying|passed away",
    "refund": r"refund\w*|non-refundable|money back",
    "baggage": r"bags?|baggage|luggage|suitcases?|carry-on|overweight|batter(?:y|ies)|"
               r"power banks?",
    "changes": r"chang\w+|standby|stand by|misspell\w*|transfer\w*|name",
    "disruption": r"delay\w*|cancels|cancell?ed (?:my|the) flight|connection|connecting|"
                  r"denied boarding|oversold|overbooked|rebook\w*",
    "checkin": r"check[- ]?in|boarding|board|gate|passport|seats?|documents",
    "special": r"pets?|cats?|dogs?|child\w*|unaccompanied|wheelchair|mobility|"
               r"service animals?|emotional support|oxygen",
    "loyalty": r"points|status|award|member",
}


def route(question: str) -> str | None:
    """A policy type, OUT_OF_SCOPE, or None when the question fits no type or two equally."""
    text = question.lower()
    weight = {}
    for name, pattern in ROUTES.items():
        hits = set(re.findall(rf"(?<![\w-])(?:{pattern})(?![\w-])", text))
        weight[name] = sum(len(hit.split()) for hit in hits)    # a longer phrase says more
    if weight[OUT_OF_SCOPE]:
        return OUT_OF_SCOPE
    best = max(weight.values())
    winners = [name for name, w in weight.items() if w == best]
    return winners[0] if best and len(winners) == 1 else None


def labeled(text: str, cited: tuple) -> str:
    """Every answer says it came from a machine and which clause versions it rests on."""
    source = ", ".join(f"{clause_id} v{version}" if version else clause_id
                       for clause_id, version in cited)
    return f"{text}\n[AI-generated answer. Source: {source or 'none'}]"


def handoff(stage: str, intent: str | None, clauses: list[Clause], draft: Draft | None = None,
            problems: list[str] = (), tier2: bool = False) -> Outcome:
    return Outcome("handoff", stage, HANDOFF_REPLY, intent, draft, problems=tuple(problems),
                   tier2=tier2, clauses=tuple(clauses))


# Instead of this: tell the model to stay inside the context, then send what it says.
def answer_naive(llm, question: str, clauses: list[Clause]) -> Outcome:
    draft = llm.draft(question, clauses)    # system prompt: "Only answer from the context."
    return Outcome("ship", "no_gates", labeled(draft.text, draft.cited), route(question),
                   draft, draft.cited, clauses=tuple(clauses))


# Use this: a draft ships only if it clears every gate. Otherwise a human gets it.
def answer(llm, question: str, clauses: list[Clause], ticket: Ticket | None = None,
           tau: float = TAU, gates: frozenset[str] = ALL_GATES) -> Outcome:
    intent = route(question)
    if "scope" in gates:                    # gate 1: is this ours to answer at all?
        if intent == OUT_OF_SCOPE:
            return handoff("out_of_scope", intent, clauses)
        if not clauses or clauses[0].score < tau:
            return handoff("no_policy", intent, clauses)    # abstain before the model runs

    draft = llm.draft(question, clauses)

    if "commitment" in gates and COMMITMENT.search(draft.text):
        decision = eligibility(intent, ticket)  # gate 2: promises come from the backend
        if decision is None:
            return handoff("commitment", intent, clauses, draft)
        version = next((c.version for c in clauses if c.clause_id == decision.clause_id), 0)
        cited = ((decision.clause_id, version),)
        return Outcome("ship", "backend", labeled(decision.text, cited), intent, draft,
                       cited, clauses=tuple(clauses))

    problems, tier2 = [], False
    if "check" in gates:                    # gates 3 and 4: does it say what the clause says?
        problems = tier1_check(draft, clauses, intent)
        if problems:                        # tier 2 runs only when tier 1 objects
            tier2 = True
            if not judge(draft, clauses, intent).grounded:
                return handoff("answer_check", intent, clauses, draft, problems, tier2)
    return Outcome("ship", "shipped", labeled(draft.text, draft.cited), intent, draft,
                   draft.cited, tuple(problems), tier2, clauses=tuple(clauses))
