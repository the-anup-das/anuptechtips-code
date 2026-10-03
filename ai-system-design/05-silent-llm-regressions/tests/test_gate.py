"""The gate blocks what the thumbs like, and the ablation finds the prompt line that hurts."""
import evals
from ablation import ablate, harmful_lines
from evals import CAP_LINE, SYSTEM_PROMPT, evaluate
from fake_model import Model, fingerprint
from gate import Rate, gate, worse
from users import ab_test

SESSIONS = [f"s{i}" for i in range(10)]
BASELINE = {"known_answer": Rate(1936, 2000), "wrong_premise": Rate(928, 1000),
            "multi_step": Rate(487, 500)}


def test_candidate_that_holds_every_suite_ships():
    candidate = {"known_answer": Rate(1929, 2000), "wrong_premise": Rate(921, 1000),
                 "multi_step": Rate(481, 500)}                      # a little lower: chance
    assert gate(candidate, BASELINE) == gate(BASELINE, BASELINE)
    assert gate(candidate, BASELINE).ship


def test_sycophantic_candidate_is_blocked_however_good_its_accuracy():
    candidate = {"known_answer": Rate(1951, 2000), "wrong_premise": Rate(532, 1000),
                 "multi_step": Rate(489, 500)}
    verdict = gate(candidate, BASELINE)
    assert not verdict.ship
    assert verdict.reasons == ("wrong_premise: 53.2% vs 92.8% baseline",)


def test_missing_suite_failed_guardrail_and_unfinished_soak_all_block():
    partial = {"known_answer": Rate(1936, 2000), "multi_step": Rate(487, 500)}
    assert gate(partial, BASELINE).reasons == ("wrong_premise: not run",)
    arms = {"corrects_wrong_premise": (Rate(310, 600), Rate(27_300, 29_400))}
    verdict = gate(BASELINE, BASELINE, guardrails=arms, soaked=False)
    assert verdict.reasons == ("A/B guardrail corrects_wrong_premise: 51.7% vs 92.9%",
                               "soak period not over")
    healthy_arms = {"corrects_wrong_premise": (Rate(552, 600), Rate(27_300, 29_400))}
    assert gate(BASELINE, BASELINE, guardrails=healthy_arms).ship


def test_worse_needs_more_than_chance_and_more_than_the_tolerance():
    assert not worse(Rate(45, 50), Rate(960, 1000))        # 90% of 50: could be luck
    assert worse(Rate(900, 1000), Rate(960, 1000))         # 90% of 1,000: it isn't
    assert not worse(Rate(9520, 10_000), Rate(960, 1000))  # 0.8 points down: inside tolerance
    assert worse(Rate(94_000, 100_000), Rate(960, 1000))   # 2 points down, and sure of it


def test_sample_size_sets_the_smallest_drop_the_gate_can_see():
    baseline = Rate(970, 1000)

    def smallest_drop(n: int) -> float:
        """Smallest observed drop, in points, that worse() flags with n replies."""
        passed = max(k for k in range(n + 1) if worse(Rate(k, n), baseline))
        return round(97 - 100 * passed / n, 2)

    assert [smallest_drop(n) for n in (500, 1000, 5000)] == [3.8, 2.9, 1.84]


def test_thumbs_prefer_the_sycophant_and_the_gate_still_says_no():
    control, candidate = Model("model-1"), Model("cand-3", agree_bias=3.0)
    arms = ab_test(control, candidate, SYSTEM_PROMPT, seed="t", users=10_000, turns=5, percent=5)
    assert arms["candidate"]["thumbs_up"].value > arms["control"]["thumbs_up"].value + 0.03
    baseline = evaluate(control, SYSTEM_PROMPT, SESSIONS)
    offline = evaluate(candidate, SYSTEM_PROMPT, SESSIONS)
    assert not worse(offline["known_answer"], baseline["known_answer"])   # accuracy looks fine
    verdict = gate(offline, baseline, guardrails={
        name: (arms["candidate"][name], arms["control"][name])
        for name in ("corrects_wrong_premise",)})
    assert not verdict.ship
    assert [r.split(":")[0] for r in verdict.reasons] == [
        "wrong_premise", "A/B guardrail corrects_wrong_premise"]


def test_eval_runs_round_trip_through_postgres(conn):
    scores = evaluate(Model(), SYSTEM_PROMPT, SESSIONS[:2])
    evals.save(conn, "run-1", Model(), SYSTEM_PROMPT, scores)
    evals.save(conn, "run-1", Model(), SYSTEM_PROMPT, scores)       # saving twice is harmless
    assert evals.load(conn, "run-1", "model-1", fingerprint(SYSTEM_PROMPT)) == scores
    assert evals.load(conn, "run-2", "model-1", fingerprint(SYSTEM_PROMPT)) == {}


def test_ablation_finds_the_cap_line_for_both_models():
    prompt = SYSTEM_PROMPT[:3] + [CAP_LINE] + SYSTEM_PROMPT[3:]
    for model in (Model("model-1", skill=4.6), Model("model-2", skill=4.9)):
        results = ablate(model, prompt, SESSIONS)
        assert len(results) == len(prompt) + 1
        assert harmful_lines(results) == [(CAP_LINE, "multi_step")]


def test_prompt_change_is_gated_per_model():
    for model in (Model("model-1", skill=4.6), Model("model-2", skill=4.9)):
        before = evaluate(model, SYSTEM_PROMPT, SESSIONS)
        after = evaluate(model, SYSTEM_PROMPT + [CAP_LINE], SESSIONS)
        verdict = gate(after, before)
        assert not verdict.ship and verdict.reasons[0].startswith("multi_step:")
