"""The two stand-ins the tests and measurements rely on: the hashing embedder and the
seeded synthetic vectors. Both have to be deterministic, or nothing else here is."""
import math

import numpy as np

import synth
from embedder import DIM, HashingEmbedder


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))   # both are unit vectors


def test_the_hashing_embedder_is_deterministic_and_unit_length():
    first, second = HashingEmbedder()(["Refund policy: 30 days"] * 2)
    assert first == second and len(first) == DIM
    assert math.isclose(sum(v * v for v in first), 1.0, rel_tol=1e-9)


def test_texts_that_share_words_are_closer_than_texts_that_do_not():
    embed = HashingEmbedder()
    question, refund, leave = embed(["refund limit for EU customers",
                                     "Refund policy / EU customers: The limit is 30 days.",
                                     "Parental leave: employees get 26 weeks."])
    assert cosine(question, refund) > 0.5 > cosine(question, leave)


def test_plurals_and_case_fold_together_and_stopwords_are_ignored():
    embed = HashingEmbedder()
    assert embed(["the Refunds"]) == embed(["refund"])


def test_text_without_usable_words_still_gets_a_vector_pgvector_can_index():
    (vector,) = HashingEmbedder()(["the of and"])
    assert sum(v * v for v in vector) == 1.0


def test_the_embedder_counts_what_it_embeds():
    embed = HashingEmbedder()
    embed(["a b", "c d"])
    embed(["e"])
    assert embed.calls == 3


def test_synthetic_vectors_are_the_same_for_the_same_seed():
    assert np.array_equal(synth.clustered(50, seed=7), synth.clustered(50, seed=7))
    assert not np.array_equal(synth.clustered(50, seed=7), synth.clustered(50, seed=8))


def test_synthetic_vectors_are_unit_length_and_clustered():
    vectors, labels = synth.clustered(400, seed=7, return_labels=True)
    assert vectors.shape == (400, synth.DIM)
    assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0, atol=1e-5)
    similarity = vectors @ vectors.T
    same = labels[:, None] == labels[None, :]
    off_diagonal = ~np.eye(400, dtype=bool)
    assert similarity[same & off_diagonal].mean() > 0.6 > similarity[~same].mean()


def test_to_text_is_what_pgvector_reads():
    assert synth.to_text(np.array([0.5, -0.25, 0.0])) == "[0.5,-0.25,0]"
