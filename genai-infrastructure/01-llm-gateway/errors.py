"""What an upstream failure means, and what the gateway does about it."""
import email.utils
import enum
import time
from collections.abc import Mapping
from dataclasses import dataclass


class Action(enum.Enum):
    RETRY = "retry"        # same provider, after a wait
    FAILOVER = "failover"  # next provider, now
    STOP = "stop"          # next provider, and keep this one out for a long while
    FAIL = "fail"          # the caller's request is wrong: hand the error back


@dataclass(frozen=True)
class Verdict:
    action: Action
    reason: str
    retry_after: float | None = None  # seconds, from the provider's Retry-After header


# Error codes that mean "this account is out of money", not "slow down".
SPEND_CODES = {
    "enforced_spend_limit_reached",       # Anthropic's tier cap: a 429 with no retry-after
    "organization_spend_limit_exceeded",  # OpenAI's enforced limits: also 429s
    "project_spend_limit_exceeded",
    "organization_usage_limit_exceeded",
    "credit_balance_exhausted",           # OpenAI: no prepaid credits left, a 429 as well
}
USER_LIMIT_PREFIX = "You have reached your specified"  # Anthropic: a limit you set, sent as a 400


def retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    """Retry-After is a number of seconds or an HTTP date (RFC 9110)."""
    value = headers.get("retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


def classify(status: int, headers: Mapping[str, str], body: object) -> Verdict:
    """Turn one upstream response into a decision. Reads both error shapes:
    OpenAI's error.code and Anthropic's error.details.error_code."""
    error = body.get("error") if isinstance(body, dict) else None
    error = error if isinstance(error, dict) else {}
    details = error.get("details") if isinstance(error.get("details"), dict) else {}
    code = error.get("code") or details.get("error_code") or ""
    wait = retry_after_seconds(headers)

    if code in SPEND_CODES:
        return Verdict(Action.STOP, code)
    if status == 400 and str(error.get("message") or "").startswith(USER_LIMIT_PREFIX):
        return Verdict(Action.STOP, "user_spend_limit")
    if status in (401, 403, 404):  # the gateway's own key or model ID is wrong, not the caller
        return Verdict(Action.STOP, code or f"provider_{status}")
    if code == "slow_down":        # ramped up too fast: don't push harder, go elsewhere
        return Verdict(Action.FAILOVER, code, wait)
    if status == 429:
        return Verdict(Action.RETRY, code or "rate_limited", wait)
    if status == 408 or status >= 500:
        return Verdict(Action.RETRY, code or f"http_{status}", wait)
    return Verdict(Action.FAIL, code or f"http_{status}")


TIMEOUT = Verdict(Action.FAILOVER, "timeout")                # nothing arrived in time
FIRST_TOKEN_TIMEOUT = Verdict(Action.FAILOVER, "first_token_timeout")
CONNECTION = Verdict(Action.FAILOVER, "connection_error")    # refused, reset, cut off
