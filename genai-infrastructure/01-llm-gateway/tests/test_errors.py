"""Which upstream errors deserve a retry: one test per row of the article's table.
The error bodies are the mock's copies of the providers' documented shapes."""
import email.utils
import time

import pytest

from errors import Action, classify, retry_after_seconds
from mock_upstream import ERRORS


def verdict(name: str):
    status, headers, body = ERRORS[name]
    return classify(status, headers, body)


def test_429_with_retry_after_is_retried_after_that_long():
    v = verdict("rate_limit_429")
    assert (v.action, v.retry_after) == (Action.RETRY, 1.0)


def test_429_without_retry_after_is_retried_with_backoff():
    v = classify(429, {}, {"error": {"type": "rate_limit_error", "message": "slow"}})
    assert (v.action, v.retry_after) == (Action.RETRY, None)


def test_slow_down_fails_over_instead_of_pushing_harder():
    v = verdict("slow_down_429")
    assert (v.action, v.reason, v.retry_after) == (Action.FAILOVER, "slow_down", 5.0)


def test_anthropic_spend_cap_429_has_no_retry_after_and_means_stop():
    status, headers, body = ERRORS["anthropic_spend_cap_429"]
    assert status == 429 and "retry-after" not in headers
    v = classify(status, headers, body)
    assert (v.action, v.reason) == (Action.STOP, "enforced_spend_limit_reached")


@pytest.mark.parametrize("code", ["organization_spend_limit_exceeded",
                                  "project_spend_limit_exceeded",
                                  "organization_usage_limit_exceeded",
                                  "credit_balance_exhausted"])
def test_openai_spend_limit_429s_mean_stop(code):
    v = classify(429, {}, {"error": {"type": "rate_limit_error", "code": code, "message": "x"}})
    assert (v.action, v.reason) == (Action.STOP, code)


def test_anthropic_user_set_limit_is_a_400_and_means_stop():
    v = verdict("anthropic_user_limit_400")
    assert (v.action, v.reason) == (Action.STOP, "user_spend_limit")


def test_an_ordinary_400_is_the_callers_problem():
    v = verdict("bad_request_400")
    assert v.action is Action.FAIL


@pytest.mark.parametrize("status", [500, 502, 503, 504, 529, 408])
def test_server_errors_and_timeouts_are_retried(status):
    assert classify(status, {}, {}).action is Action.RETRY


def test_overloaded_errors_keep_the_providers_code_as_the_reason():
    assert verdict("overloaded_503").reason == "server_is_overloaded"
    assert verdict("anthropic_overloaded_529").action is Action.RETRY


@pytest.mark.parametrize("status", [401, 403, 404])
def test_a_refused_key_or_unknown_model_takes_the_provider_out(status):
    # The caller sent a virtual key and a route name. If the provider rejects the real
    # key or the model ID, that is the gateway's configuration, not the caller's request.
    assert classify(status, {}, {"error": {"message": "no"}}).action is Action.STOP


def test_other_4xx_go_back_to_the_caller():
    assert classify(422, {}, {"error": {"message": "bad tool schema"}}).action is Action.FAIL
    assert classify(413, {}, "not json").action is Action.FAIL


def test_retry_after_can_be_seconds_or_an_http_date():
    assert retry_after_seconds({"retry-after": "7"}) == 7.0
    assert retry_after_seconds({}) is None
    assert retry_after_seconds({"retry-after": "soon"}) is None
    in_30s = email.utils.formatdate(time.time() + 30, usegmt=True)
    assert 28 <= retry_after_seconds({"retry-after": in_30s}) <= 30
