"""The idea in 50 lines: routes tried in order, retries with backoff, a budget check.

This is the sketch the article started from. It is synchronous and keeps everything in
memory, so it leaves out what the rest of this folder is about: the budget check below
is not safe under concurrency (see budget.py), nothing stops a dead provider from being
called again (breaker.py), and it can't stream (upstream.py).
"""
import random
import time
from dataclasses import dataclass
from typing import Callable


class RetryableError(Exception):  # 429s, 5xx errors, timeouts
    def __init__(self, msg: str, retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after  # seconds, from Retry-After


@dataclass
class Route:
    name: str
    call: Callable[[dict], tuple[str, dict]]  # adapter: returns (text, usage)
    usd_per_1k_in: float  # from your provider's current price list
    usd_per_1k_out: float

    def cost(self, tokens_in: int, tokens_out: int) -> float:
        return (tokens_in * self.usd_per_1k_in
                + tokens_out * self.usd_per_1k_out) / 1000


@dataclass
class Budget:
    limit_usd: float
    spent_usd: float = 0.0


def complete_with_fallback(request, routes, budget,
                           attempts=3, base=0.5, cap=8.0):
    failures = []
    for route in routes:
        worst = route.cost(request["input_tokens"], request["max_tokens"])
        if budget.spent_usd + worst > budget.limit_usd:
            failures.append(f"{route.name}: over budget")
            continue
        for attempt in range(attempts):
            try:
                text, usage = route.call(request)
            except RetryableError as err:
                wait = err.retry_after
                if wait is None:  # exponential backoff with full jitter
                    wait = random.uniform(0, min(cap, base * 2 ** attempt))
                if attempt == attempts - 1 or wait > cap:
                    failures.append(f"{route.name}: {err}")
                    break  # give up on this route, fall back
                time.sleep(wait)
                continue
            budget.spent_usd += route.cost(usage["input_tokens"],
                                           usage["output_tokens"])
            return {"route": route.name, "text": text, "usage": usage}
    raise RuntimeError("all routes failed: " + "; ".join(failures))
