"""The kill switch, on every action it can touch."""
import pytest

from killswitch import Action, Rule, evaluate, evaluate_naive


def rulesets(action: Action) -> dict[str, list[Rule]]:
    """A top-level ruleset with one rule of the given action. EXECUTE runs the test rules."""
    target = "test-rules" if action is Action.EXECUTE else None
    return {"root": [Rule("r1", action, ruleset=target)],
            "test-rules": [Rule("t1", Action.LOG)]}


@pytest.mark.parametrize("killed", [False, True], ids=["live", "killed"])
@pytest.mark.parametrize("action", list(Action), ids=lambda action: action.value)
def test_the_kill_switch_works_on_every_action(action, killed):
    [result] = evaluate(rulesets(action), "root", killed={"r1"} if killed else set())
    assert result.skipped is killed
    ran_the_sub_ruleset = action is Action.EXECUTE and not killed
    assert (result.results is not None) is ran_the_sub_ruleset


@pytest.mark.parametrize("killed", [False, True], ids=["live", "killed"])
@pytest.mark.parametrize("action", list(Action), ids=lambda action: action.value)
def test_the_naive_engine_breaks_only_when_an_execute_rule_is_killed(action, killed):
    killed_rules = {"r1"} if killed else set()
    if action is Action.EXECUTE and killed:
        with pytest.raises(AttributeError,
                           match="'NoneType' object has no attribute 'results_index'"):
            evaluate_naive(rulesets(action), "root", killed_rules)
    else:
        evaluate_naive(rulesets(action), "root", killed_rules)


def test_a_live_execute_rule_returns_the_sub_rulesets_results():
    [result] = evaluate(rulesets(Action.EXECUTE), "root", killed=set())
    assert [sub.rule for sub in result.results] == ["t1"]


def test_killing_a_rule_inside_the_sub_ruleset():
    [result] = evaluate(rulesets(Action.EXECUTE), "root", killed={"t1"})
    assert result.skipped is False and result.results[0].skipped is True
