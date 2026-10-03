"""A deterministic stand-in for an LLM, small enough to read in one sitting.

Logits come from a hash of (model, system prompt, probe, session), so the same request always
gets the same reply. Sampling is the usual chain: top-k -> softmax -> top-p -> greedy at
temperature 0. The knobs on Model and Serving are where the lab injects its bugs.
"""
import hashlib
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np

VOCAB = 100                 # token ids
LENGTH = 12                 # tokens per reply; token 0 carries the answer
RARE = slice(0, 8)          # ids 0-7 stand in for another script (Thai, Chinese)
AGREE, PUSH_BACK = 8, 9     # how a reply to a wrong premise starts
CORRUPT_RATE = 0.004        # share of token positions the script-boost bug touches
BUDGET = 400                # buckets the approximate top-k can afford per step, for the whole batch
CAP = re.compile(r"between tool calls to (\d+) words or fewer")


def _seed(*parts: object) -> int:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def unit(*parts: object) -> float:
    """A reproducible number in [0, 1) for these parts."""
    return _seed(*parts) / 2**64


@dataclass(frozen=True)
class Probe:
    id: str
    suite: str                   # known_answer | wrong_premise | multi_step
    prompt: str
    answer: int                  # the token a good reply starts with
    steps: tuple[int, ...] = ()  # multi_step: words of reasoning each step needs

    @property
    def foreign(self) -> bool:   # a translation probe: the answer is in the other script
        return self.answer < RARE.stop


@dataclass(frozen=True)
class Request:
    probe: Probe
    session: str                 # the conversation it arrives in


@dataclass(frozen=True)
class Model:
    version: str = "model-1"
    skill: float = 4.6           # how far the right token's logit sits above the noise
    agree_bias: float = 0.0      # extra logit for agreeing with the user's premise


def exact_top_k(logits: np.ndarray, k: int) -> np.ndarray:
    """Mask of the k highest logits in each row."""
    kth = np.partition(logits, -k, axis=1)[:, -k]
    return logits >= kth[:, None]


def approx_top_k(logits: np.ndarray, k: int) -> np.ndarray:
    """Approximate top-k: split each row into buckets, keep each bucket's best token, then the
    k best of those. The best token overall always wins its bucket, so only low ranks get lost.
    Bigger batches get fewer, wider buckets, to keep the cost per step flat."""
    batch, vocab = logits.shape
    buckets = max(k, min(vocab, BUDGET // batch))
    width = vocab // buckets  # BUG (injected): floor, so the last vocab % buckets ids are skipped
    scanned = logits[:, :buckets * width].reshape(batch, buckets, width)
    winners = scanned.argmax(axis=2) + np.arange(buckets) * width
    keep = np.zeros(logits.shape, dtype=bool)
    np.put_along_axis(keep, winners, True, axis=1)
    return keep & exact_top_k(np.where(keep, logits, -np.inf), k)


@dataclass(frozen=True)
class Serving:
    """What the server that runs the request is configured to do. Defaults are healthy."""
    top_k: Callable[[np.ndarray, int], np.ndarray] = exact_top_k
    dtype: type = np.float32     # precision of the final pick
    noise: float = 0.0           # extra logit noise: the misconfigured pool
    script_boost: float = 0.0    # an "optimization" that lifts rare-script tokens


def softmax(x: np.ndarray) -> np.ndarray:
    e = np.exp(x - x.max(axis=1, keepdims=True))
    return e / e.sum(axis=1, keepdims=True)


def nucleus(probs: np.ndarray, p: float) -> np.ndarray:
    """Top-p mask: the most likely tokens, up to and including the one that crosses p."""
    order = np.argsort(-probs, axis=1)
    ranked = np.take_along_axis(probs, order, axis=1)
    ahead = np.cumsum(ranked, axis=1) - ranked      # probability mass ranked above each token
    keep = np.zeros(probs.shape, dtype=bool)
    np.put_along_axis(keep, order, ahead < p, axis=1)
    return keep


def sample(logits: np.ndarray, serving: Serving, k: int = 5, p: float = 0.99) -> np.ndarray:
    """One token id per row: greedy at temperature 0 over what survives top-k, then top-p."""
    keep = serving.top_k(logits, k)
    keep &= nucleus(softmax(np.where(keep, logits, -np.inf)), p)
    return np.where(keep, logits, -np.inf).astype(serving.dtype).argmax(axis=1)


def fingerprint(system_prompt: Sequence[str]) -> str:
    """The prompt's version: change one character and you have a new one."""
    return hashlib.sha256("\n".join(system_prompt).encode()).hexdigest()[:8]


def words_cap(system_prompt: Sequence[str]) -> int | None:
    """The one instruction the stand-in obeys: a cap on words between tool calls."""
    for line in system_prompt:
        if found := CAP.search(line):
            return int(found.group(1))
    return None


def _thought_through(req: Request, cap: int | None) -> bool:
    """A multi-step task needs room to reason. A step cut short by the cap may be lost."""
    for i, need in enumerate(req.probe.steps):
        if cap is not None and need > cap:
            if unit("step", req.probe.id, req.session, i) >= cap / need:   # the bigger the cut,
                return False                                               # the likelier the loss
    return True


def _logits(model: Model, serving: Serving, prompt_id: str, cap: int | None,
            req: Request) -> np.ndarray:
    probe = req.probe
    rng = np.random.default_rng(_seed(model.version, prompt_id, probe.id, req.session))
    x = rng.standard_normal((LENGTH, VOCAB), dtype=np.float32)
    corrupt = rng.random(LENGTH) < CORRUPT_RATE  # always drawn: a bug toggle changes nothing else
    if not probe.foreign:
        x[:, RARE] -= 8.0                        # another script: should rarely be produced
    if probe.suite == "wrong_premise":
        x[0, PUSH_BACK] += model.skill
        x[0, AGREE] += model.skill - 2.1 + model.agree_bias
    elif _thought_through(req, cap):
        x[0, probe.answer] += model.skill        # the model knows this one
    if serving.noise:
        x += serving.noise * rng.standard_normal(x.shape, dtype=np.float32)
    if serving.script_boost:
        x[corrupt, RARE] += serving.script_boost
    return x


def respond(model: Model, serving: Serving, system_prompt: Sequence[str],
            batch: Sequence[Request]) -> np.ndarray:
    """One forward pass over `batch`: token ids, shape (len(batch), LENGTH)."""
    prompt_id, cap = fingerprint(system_prompt), words_cap(system_prompt)
    logits = np.stack([_logits(model, serving, prompt_id, cap, req) for req in batch])
    return np.stack([sample(logits[:, pos], serving) for pos in range(LENGTH)], axis=1)
