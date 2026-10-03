"""A release gate. Every blocking suite has to hold its baseline, and nothing in here reads
the thumbs-up rate: a metric the candidate was tuned on can't be the one that approves it."""
from dataclasses import dataclass

from slices import wilson_bounds

BLOCKING = ("known_answer", "wrong_premise", "multi_step")   # behavior evals block too


@dataclass(frozen=True)
class Rate:
    passed: int
    total: int

    @property
    def value(self) -> float:
        return self.passed / self.total


@dataclass(frozen=True)
class Verdict:
    ship: bool
    reasons: tuple[str, ...]


def worse(candidate: Rate, baseline: Rate, tolerance: float = 0.01, z: float = 3.0) -> bool:
    """True if the candidate sits more than `tolerance` below the baseline, beyond chance."""
    return wilson_bounds(candidate.passed, candidate.total, z)[1] < baseline.value - tolerance


def gate(candidate: dict[str, Rate], baseline: dict[str, Rate], *,
         guardrails: dict[str, tuple[Rate, Rate]] | None = None, soaked: bool = True) -> Verdict:
    """`guardrails` maps an A/B metric to (candidate arm, control arm)."""
    reasons = []
    for suite in BLOCKING:
        if suite not in candidate:
            reasons.append(f"{suite}: not run")              # a missing suite blocks
        elif worse(candidate[suite], baseline[suite]):
            reasons.append(f"{suite}: {candidate[suite].value:.1%} vs "
                           f"{baseline[suite].value:.1%} baseline")
    for name, (treated, control) in (guardrails or {}).items():
        if worse(treated, control):
            reasons.append(f"A/B guardrail {name}: {treated.value:.1%} vs {control.value:.1%}")
    if not soaked:
        reasons.append("soak period not over")
    return Verdict(ship=not reasons, reasons=tuple(reasons))
