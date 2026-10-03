"""The stand-in model and its injected bugs."""
import numpy as np

from evals import CAP_LINE, SYSTEM_PROMPT, evaluate
from fake_model import (AGREE, LENGTH, PUSH_BACK, RARE, VOCAB, Model, Request, Serving,
                        approx_top_k, exact_top_k, fingerprint, nucleus, respond, sample, softmax,
                        words_cap)
from probes import PROBES, by_suite, score

SESSIONS = [f"s{i}" for i in range(10)]
KNOWN = by_suite("known_answer")


def test_probe_set_has_the_three_suites():
    assert len(KNOWN) == 200
    assert len(by_suite("wrong_premise")) == 100
    assert len(by_suite("multi_step")) == 50
    assert len({p.id for p in PROBES}) == 350
    assert sum(p.foreign for p in KNOWN) == 4          # the translation probes


def test_same_request_same_reply():
    batch = [Request(p, "s0") for p in KNOWN[:16]]
    first = respond(Model(), Serving(), SYSTEM_PROMPT, batch)
    assert first.shape == (16, LENGTH)
    assert (respond(Model(), Serving(), SYSTEM_PROMPT, batch) == first).all()


def test_healthy_model_knows_most_answers_but_not_all():
    rate = evaluate(Model(), SYSTEM_PROMPT, SESSIONS, probes=KNOWN)["known_answer"].value
    assert 0.94 < rate < 0.99


def test_sample_is_greedy_over_what_survives_top_k_and_top_p():
    logits = np.array([[0.1, 3.0, 2.9, -1.0, 0.5, 0.0]], dtype=np.float32)
    assert sample(logits, Serving(), k=3).tolist() == [1]
    assert exact_top_k(logits, 3).tolist() == [[False, True, True, False, True, False]]
    probs = softmax(logits)
    assert nucleus(probs, 0.5).tolist() == [[False, True, True, False, False, False]]
    assert nucleus(probs, 0.3).tolist() == [[False, True, False, False, False, False]]


def test_approx_top_k_matches_exact_on_the_top_token_when_buckets_divide_the_vocab():
    rng = np.random.default_rng(1)
    for batch in (1, 8):    # 100 and 50 buckets: every token id is scanned
        logits = rng.standard_normal((batch, VOCAB)).astype(np.float32)
        assert (sample(logits, Serving(top_k=approx_top_k)) == logits.argmax(axis=1)).all()


def test_approx_top_k_drops_the_top_token_at_some_batch_sizes():
    for batch, buckets in ((32, 12), (64, 6)):
        logits = np.zeros((batch, VOCAB), dtype=np.float32)
        logits[:, 97] = 5.0      # the best token sits in the last vocab % buckets ids
        logits[:, 40] = 4.0
        assert VOCAB % buckets == 4
        assert not approx_top_k(logits, 5)[:, 97].any()
        assert (sample(logits, Serving(top_k=approx_top_k)) == 40).all()
        assert (sample(logits, Serving()) == 97).all()        # the exact kernel keeps it


def test_fp16_pick_flips_a_near_tie_that_fp32_gets_right():
    logits = np.zeros((1, VOCAB), dtype=np.float32)
    logits[0, 30], logits[0, 60] = 4.0005, 4.0010      # closer than float16 can tell apart
    assert sample(logits, Serving()).tolist() == [60]
    assert sample(logits, Serving(dtype=np.float16)).tolist() == [30]


def test_misconfigured_pool_answers_worse():
    healthy = evaluate(Model(), SYSTEM_PROMPT, SESSIONS, probes=KNOWN)["known_answer"]
    noisy = evaluate(Model(), SYSTEM_PROMPT, SESSIONS, serving=Serving(noise=1.0),
                     probes=KNOWN)["known_answer"]
    assert noisy.value < healthy.value - 0.10


def test_agree_bias_trades_corrections_for_agreement_and_leaves_accuracy_alone():
    premise = by_suite("wrong_premise")
    rates = []
    for bias in (0.0, 2.0, 4.0):
        scores = evaluate(Model("m", agree_bias=bias), SYSTEM_PROMPT, SESSIONS)
        rates.append(scores["wrong_premise"].value)
        assert scores["known_answer"].value > 0.94       # same model version: same accuracy
    assert rates[0] > 0.85 and rates[1] < rates[0] - 0.2 and rates[2] < 0.2
    batch = [Request(p, "s0") for p in premise[:32]]
    first = respond(Model("m", agree_bias=4.0), Serving(), SYSTEM_PROMPT, batch)[:, 0]
    assert set(first.tolist()) <= {AGREE, PUSH_BACK}


def test_words_cap_is_read_from_the_system_prompt():
    assert words_cap(SYSTEM_PROMPT) is None
    assert words_cap(SYSTEM_PROMPT + [CAP_LINE]) == 25
    assert fingerprint(SYSTEM_PROMPT) != fingerprint(SYSTEM_PROMPT + [CAP_LINE])


def test_word_cap_hurts_multi_step_probes_only():
    full = evaluate(Model(), SYSTEM_PROMPT, SESSIONS)
    capped = evaluate(Model(), SYSTEM_PROMPT + [CAP_LINE], SESSIONS)
    assert capped["multi_step"].value < full["multi_step"].value - 0.05
    assert abs(capped["known_answer"].value - full["known_answer"].value) < 0.03


def test_script_boost_puts_rare_tokens_in_replies_and_barely_moves_the_pass_rate():
    batch = [Request(p, s) for s in SESSIONS for p in KNOWN if not p.foreign]

    def run(serving: Serving) -> np.ndarray:
        return np.concatenate([respond(Model(), serving, SYSTEM_PROMPT, batch[i:i + 8])
                               for i in range(0, len(batch), 8)])

    clean, boosted = run(Serving()), run(Serving(script_boost=12.0))
    assert not (clean < RARE.stop).any()
    hit = (boosted < RARE.stop).any(axis=1)
    assert 0.02 < hit.mean() < 0.08
    passed = [np.mean([score(r.probe, t) for r, t in zip(batch, out)]) for out in (clean, boosted)]
    assert 0 <= passed[0] - passed[1] < 0.01
