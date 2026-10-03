"""The answer checks: tier 1 (cheap, on every draft), the tier-2 judge stand-in (only on
drafts tier 1 flags) and the commitment pattern. Pure Python, no I/O.
"""
import re

from models import Clause, Draft, Verdict

THETA = 0.75        # tier 1: share of a sentence's words that must come from the clause
JUDGE_THETA = 0.6   # tier 2 tolerates more rewording

STOP = frozenset(
    "a about all also am an and any are as at be been both but by can could did do does each "
    "for from had has have how i if in into is it its may me must my of on once only or our "
    "per shall should so such than that the their them then there these they this those to "
    "too up us was we were what when where which who whom why will with would yes you your"
    .split())
WORD = re.compile(r"[a-z]+(?:[-'][a-z]+)*")
SENTENCE = re.compile(r"(?<=[.!?])\s+")


def sentences(text: str) -> list[str]:
    return [s for s in SENTENCE.split(text.strip()) if s]


# A number with its unit, if it has one: $35, 90 days, 24-hour, 23 kg, 25,000 status points
FACT = re.compile(r"\$?\d[\d,]*(?:\.\d+)?(?:[- ](?:kg|days?|hours?|minutes?|months?|years?|"
                  r"characters|business days|status points|points))?")
NEGATION = re.compile(r"\b(?:not|never|cannot|non-\w+)\b|n't\b")


def facts(text: str) -> set[str]:
    """Every number in the text with its unit: {'90 day', '$35', '23 kg'}."""
    found = (m.replace(",", "").replace("-", " ") for m in FACT.findall(text.lower()))
    return {m.removesuffix("s") for m in found}


def negations(text: str) -> int:
    return len(NEGATION.findall(text.lower()))


def stems(text: str) -> set[str]:
    """Content words, roughly stemmed: 'refunded', 'refunds' and 'refundable' all match."""
    out = set()
    for word in WORD.findall(NEGATION.sub(" ", text.lower())):
        if word in STOP:
            continue
        word = word.removesuffix("'s")
        for suffix in ("able", "ing", "ed", "s"):
            if word.endswith(suffix) and len(word) - len(suffix) >= 3 and word[-2:] != "ss":
                word = word[: -len(suffix)]
                break
        if len(word) > 3 and word[-1] == word[-2] and word[-1] in "bdglmnprt":
            word = word[:-1]            # cancell(ed) -> cancel
        out.add(word.removesuffix("e") if len(word) > 3 else word)
    return out


def overlap(claim: str, source: str) -> float:
    """Share of the claim's content words that the source sentence also has."""
    words = stems(claim)
    return len(words & stems(source)) / len(words) if words else 0.0


def cited_clauses(draft: Draft, clauses: list[Clause]) -> list[Clause] | None:
    """The clauses the draft cites, or None if it cites nothing that is in force."""
    in_force = {(c.clause_id, c.version): c for c in clauses}
    cited = [in_force.get(ref) for ref in draft.cited]
    return None if not cited or None in cited else cited


def tier1_check(draft: Draft, clauses: list[Clause], intent: str | None) -> list[str]:
    """Cheap checks on a draft. Returns what failed; an empty list means it can ship."""
    cited = cited_clauses(draft, clauses)
    if cited is None:
        return ["citation"]             # no clause, or a version that isn't in force
    problems = set()
    if any(c.policy_type != intent for c in cited):
        problems.add("policy_type")     # a real clause from the wrong part of the policy
    source = [s for c in cited for s in sentences(c.body)]
    for sentence in sentences(draft.text):
        best = max(source, key=lambda s: overlap(sentence, s))
        if facts(sentence) - facts(best):
            problems.add("facts")       # a number or a duration the clause doesn't have
        if overlap(sentence, best) < THETA:
            problems.add("wording")     # too far from anything the clause says
        elif facts(best) - facts(sentence) or negations(sentence) != negations(best):
            problems.add("facts")       # close to the clause, but a number or a "not" is gone
    return sorted(problems)


SMALL_TALK = re.compile(r"^(?:thanks|thank you|happy to help|glad to help|good question|sure|"
                        r"of course|i hope|hope this helps|let me know|is there anything|"
                        r"feel free|here(?:'s| is) what)\b", re.I)


def judge(draft: Draft, clauses: list[Clause], intent: str | None) -> Verdict:
    """Stand-in for the LLM evaluator. It has no model inside: it applies tier 1's fact
    checks again, but skips small talk and accepts more rewording, the two things a real
    evaluator is better at than a word-overlap check. Swap in a model call here."""
    cited = cited_clauses(draft, clauses)
    if cited is None:
        return Verdict(False, "cites no clause that is in force")
    if any(c.policy_type != intent for c in cited):
        return Verdict(False, "the cited clause is from another part of the policy")
    source = [s for c in cited for s in sentences(c.body)]
    claims = [s for s in sentences(draft.text) if not SMALL_TALK.match(s)]
    for claim in claims:
        best = max(source, key=lambda s: overlap(claim, s))
        if facts(claim) != facts(best) or negations(claim) != negations(best):
            return Verdict(False, f"a number or a negation differs from the clause: {claim}")
        if overlap(claim, best) < JUDGE_THETA:
            return Verdict(False, f"not supported by the cited clause: {claim}")
    if not claims:
        return Verdict(False, "the answer makes no claim")
    return Verdict(True, "every claim is supported by a cited clause")


# A promise to this customer, as opposed to a statement of policy. Refunds, eligibility,
# prices and anything "binding" must come from the backend, never from generated text.
COMMITMENT = re.compile(
    r"\b(?:we will|we'll|i will|i'll|i can confirm|i confirm|you will (?:get|receive)|"
    r"you'll (?:get|receive)|you(?: are|'re) (?:eligible|entitled|owed)|"
    r"guarantee[ds]?|binding|promise[ds]?|(?:that's|we have) a deal)\b", re.I)
