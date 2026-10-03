"""A deterministic stand-in for an embedding model, so every test here runs with no model
download and no API key.

It hashes each word into one of 384 buckets (the hashing trick) and L2-normalises the counts,
so two texts that share words get similar vectors. It knows nothing about meaning. In
production this is a call to your embedding model behind the same signature.
"""
import hashlib
import math
import re
from collections.abc import Sequence

DIM = 384
STOPWORDS = frozenset(
    "a an and are as at be by can do does for from how i in is it my of on or "
    "that the to what when which who with you your".split())


class HashingEmbedder:
    name = "hashing-384-v1"  # stored with every chunk; a new model gets a new name

    def __init__(self) -> None:
        self.calls = 0  # texts embedded so far, so tests can see what was re-embedded

    def __call__(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += len(texts)
        return [self._embed(text) for text in texts]

    @staticmethod
    def _embed(text: str) -> list[float]:
        vector = [0.0] * DIM
        for word in re.findall(r"[a-z0-9]+", text.lower()):
            if word in STOPWORDS:
                continue
            if len(word) > 3 and word.endswith("s"):
                word = word[:-1]  # crude plural folding: "refunds" and "refund" share a bucket
            digest = hashlib.blake2b(word.encode(), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "big") % DIM
            vector[bucket] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0.0:  # no usable words: pgvector can't index a zero vector for cosine
            vector[0] = norm = 1.0
        return [v / norm for v in vector]
