"""The four records that travel through the bot."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Clause:
    clause_id: str
    version: int
    policy_type: str
    body: str
    score: float = 0.0           # share of the question's words this clause contains


@dataclass(frozen=True)
class Draft:
    """What the model wrote, and the clause versions it says it used."""
    text: str
    cited: tuple[tuple[str, int], ...] = ()      # (("BRV-02", 1),)
    mode: str = ""               # FakeLLM only: how this draft was made. No gate reads it.


@dataclass(frozen=True)
class Verdict:
    grounded: bool
    reason: str


@dataclass(frozen=True)
class Outcome:
    action: str                  # "ship" or "handoff"
    stage: str                   # the gate that decided
    reply: str                   # the customer's copy
    intent: str | None = None
    draft: Draft | None = None
    cited: tuple[tuple[str, int], ...] = ()
    problems: tuple[str, ...] = ()               # what tier 1 flagged
    tier2: bool = False          # was the judge called?
    cached: bool = False
    clauses: tuple[Clause, ...] = field(default=(), repr=False)
