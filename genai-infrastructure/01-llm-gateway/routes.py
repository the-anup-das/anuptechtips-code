"""The route table: callers name a task, the gateway decides which models serve it."""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    provider: str         # which upstream: a key of config.PROVIDERS
    model: str            # that provider's model ID
    timeout_s: float      # one attempt, start to finish
    first_token_s: float  # streams only: how long to wait for the first token
    max_tokens: int       # the most output one call may buy here

    @property
    def name(self) -> str:
        return f"{self.provider}/{self.model}"


@dataclass(frozen=True)
class Route:
    chain: tuple[Target, ...]  # tried in order: the primary, then its fallbacks
    cache_ttl_s: int = 0       # 0 = never cache this route's answers


ROUTES: dict[str, Route] = {
    "support-reply": Route((
        Target("alpha", "alpha-large", timeout_s=30, first_token_s=3, max_tokens=1024),
        Target("beta", "beta-large", timeout_s=30, first_token_s=3, max_tokens=1024),
    )),
    "classify": Route((
        Target("beta", "beta-small", timeout_s=5, first_token_s=2, max_tokens=64),
        Target("alpha", "alpha-small", timeout_s=5, first_token_s=2, max_tokens=64),
    ), cache_ttl_s=300),
    "summarize": Route((
        Target("alpha", "alpha-large", timeout_s=60, first_token_s=5, max_tokens=2048),
    )),
}

# USD per million tokens (input, output). These are made up for the mock providers:
# load yours from the provider's price list, and keep the version with every usage
# record, because last month's usage is priced with last month's table.
PRICES_VERSION = "2026-10-01"
PRICES: dict[str, tuple[float, float]] = {
    "alpha/alpha-large": (3.00, 15.00),
    "alpha/alpha-small": (0.50, 2.50),
    "beta/beta-large": (2.50, 10.00),
    "beta/beta-small": (0.25, 1.25),
}


def cost_micro_usd(target: Target, tokens_in: int, tokens_out: int) -> int:
    """A price in dollars per million tokens is the same number in micro-dollars per
    token, so budgets are whole micro-dollars and Redis never adds floats."""
    price_in, price_out = PRICES[target.name]
    cost = tokens_in * price_in + tokens_out * price_out
    return math.ceil(cost - 1e-9)  # round up, so a budget never under-counts
