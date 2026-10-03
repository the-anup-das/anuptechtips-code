"""Deterministic synthetic vectors for the two pgvector measurements: no model, no download.

Real embeddings cluster by topic, so these do too: `topics` random centres, a few subtopics
around each one, and every vector a noisy copy of its subtopic's centre, scaled to unit length.
The same seed always gives the same vectors.
"""
import numpy as np

DIM = 384


def clustered(n: int, seed: int, topics: int = 50, subtopics: int = 10,
              return_labels: bool = False):
    """n unit vectors of DIM dimensions in topics * subtopics clusters."""
    rng = np.random.default_rng(seed)
    centres = make_centres(topics, subtopics)
    labels = rng.integers(0, len(centres), n)
    vectors = noisy(centres[labels], rng)
    return (vectors, labels) if return_labels else vectors


def make_centres(topics: int = 50, subtopics: int = 10) -> np.ndarray:
    """The cluster centres. They never depend on the caller's seed, so documents and
    queries generated with different seeds still talk about the same topics."""
    rng = np.random.default_rng(20261001)
    topic = rng.standard_normal((topics, 1, DIM))
    centres = topic + 0.7 * rng.standard_normal((topics, subtopics, DIM))
    return centres.reshape(-1, DIM).astype(np.float32)


def noisy(centres: np.ndarray, rng: np.random.Generator, spread: float = 0.6) -> np.ndarray:
    vectors = centres + spread * rng.standard_normal(centres.shape).astype(np.float32)
    return vectors / np.linalg.norm(vectors, axis=1, keepdims=True)


def to_text(vector) -> str:
    """pgvector's text form, for COPY."""
    return "[" + ",".join(f"{value:.6g}" for value in vector) + "]"
