"""A small rules engine with a kill switch, and the bug that only shows up when the switch is
used on one kind of rule. A rule has an action; EXECUTE runs another ruleset."""
import enum
from dataclasses import dataclass


class Action(enum.Enum):
    BLOCK = "block"
    LOG = "log"
    SKIP = "skip"
    EXECUTE = "execute"                      # evaluate another ruleset


@dataclass
class Rule:
    id: str
    action: Action
    ruleset: str | None = None               # EXECUTE only: the ruleset to run


@dataclass
class Execute:
    results_index: int                       # where the sub-ruleset's results were put


@dataclass
class RuleResult:
    rule: str
    action: Action
    skipped: bool = False                    # True when the kill switch disabled the rule
    execute: Execute | None = None           # set only when an EXECUTE rule really ran
    results: list["RuleResult"] | None = None


def _run(rulesets: dict[str, list[Rule]], name: str, killed: set[str], evaluate):
    """Evaluate each rule once. A killed rule is skipped, so its sub-ruleset never runs."""
    results, ruleset_results = [], []
    for rule in rulesets[name]:
        result = RuleResult(rule.id, rule.action, skipped=rule.id in killed)
        if rule.action is Action.EXECUTE and not result.skipped:
            ruleset_results.append(evaluate(rulesets, rule.ruleset, killed))
            result.execute = Execute(results_index=len(ruleset_results) - 1)
        results.append(result)
    return results, ruleset_results


# Instead of this: "an EXECUTE rule always has an execute object"
def evaluate_naive(rulesets: dict[str, list[Rule]], name: str, killed: set[str]):
    results, ruleset_results = _run(rulesets, name, killed, evaluate_naive)
    for result in results:
        if result.action is Action.EXECUTE:
            result.results = ruleset_results[result.execute.results_index]
    return results


# Use this: ask whether the rule ran, not what kind of rule it is
def evaluate(rulesets: dict[str, list[Rule]], name: str, killed: set[str]):
    results, ruleset_results = _run(rulesets, name, killed, evaluate)
    for result in results:
        if result.execute is not None:
            result.results = ruleset_results[result.execute.results_index]
    return results
