"""concept.py, the 50-line sketch: it still does what the article says it does."""
import pytest

from concept import Budget, RetryableError, Route, complete_with_fallback

REQUEST = {"input_tokens": 1000, "max_tokens": 500}


def ok(name: str):
    return lambda request: (f"answer from {name}", {"input_tokens": 1000, "output_tokens": 200})


def down(request):
    raise RetryableError("503")


def test_falls_back_when_the_primary_keeps_failing():
    routes = [Route("primary", down, 3.0, 15.0), Route("backup", ok("backup"), 2.5, 10.0)]
    budget = Budget(limit_usd=100)
    result = complete_with_fallback(REQUEST, routes, budget, base=0.001)
    assert result["route"] == "backup"
    assert budget.spent_usd == pytest.approx((1000 * 2.5 + 200 * 10.0) / 1000)


def test_skips_a_route_whose_worst_case_breaks_the_budget():
    routes = [Route("expensive", ok("expensive"), 30.0, 150.0), Route("cheap", ok("cheap"), 0.3, 1.5)]
    budget = Budget(limit_usd=50)  # the expensive route's worst case is 30 + 75 = 105
    assert complete_with_fallback(REQUEST, routes, budget)["route"] == "cheap"


def test_a_long_retry_after_falls_back_instead_of_sleeping():
    def rate_limited(request):
        raise RetryableError("429", retry_after=60)

    calls = []
    routes = [Route("primary", lambda r: calls.append(1) or rate_limited(r), 3.0, 15.0),
              Route("backup", ok("backup"), 2.5, 10.0)]
    assert complete_with_fallback(REQUEST, routes, Budget(100))["route"] == "backup"
    assert len(calls) == 1


def test_raises_when_every_route_fails():
    with pytest.raises(RuntimeError, match="all routes failed"):
        complete_with_fallback(REQUEST, [Route("only", down, 3.0, 15.0)], Budget(100), base=0.001)
