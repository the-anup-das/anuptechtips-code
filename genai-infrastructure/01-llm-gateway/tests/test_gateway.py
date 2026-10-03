"""The request path end to end, over real HTTP: the gateway and the mock provider both
run under uvicorn. One test per failure the article describes."""
import threading
import time

import httpx
import openai
import pytest
import redis

from budget import limit_key, spent_key
from gateway import STOP_OPEN_S
from helpers import (ACME, GLOBEX, PROMPT, REDIS_URL, ask, ask_stream, body, calls, control,
                     events, http, text_of)
from routes import cost_micro_usd
from helpers import ROUTES

ALPHA, BETA = ROUTES["support-reply"].chain
rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def tenant_spent() -> int:
    return int(rds.get(spent_key("demo", "tenant:acme")) or 0)


# ---------------------------------------------------------------- the happy path

def test_a_request_goes_to_the_primary_and_is_recorded(gateway, mock_url):
    gw = gateway()
    resp = ask(gw.url)
    assert resp.status_code == 200
    assert resp.json()["choices"][0]["message"]["content"] == "the quick brown fox jumps over the lazy"
    assert resp.headers["x-llmgw-target"] == "alpha/alpha-large"
    assert (resp.headers["x-llmgw-hop"], resp.headers["x-llmgw-attempts"]) == ("0", "1")

    record, = gw.records
    assert (record["team"], record["feature"], record["tenant"]) == ("support", "refund-summary", "acme")
    assert (record["route"], record["provider"], record["model"]) == ("support-reply", "alpha", "alpha-large")
    assert (record["input_tokens"], record["output_tokens"]) == (7, 8)
    assert record["cost_micro_usd"] == cost_micro_usd(ALPHA, 7, 8) == 141
    assert record["usage_estimated"] is False and record["status"] == "ok"
    assert tenant_spent() == 141  # the worst case was reserved, the real cost is what stayed


def test_the_provider_sees_its_own_key_never_the_virtual_one(gateway, mock_url):
    ask(gateway().url)
    assert http.get(f"{mock_url}/_stats").json()["alpha"]["last_auth"] == "Bearer sk-mock-alpha"
    # And a virtual key is worthless outside the gateway:
    direct = http.post(f"{mock_url}/alpha/v1/chat/completions", json=body(), headers=ACME)
    assert direct.status_code == 401


def test_max_tokens_is_capped_by_the_route(gateway):
    resp = ask(gateway().url, body(max_tokens=5000))
    assert resp.json()["usage"]["completion_tokens"] == 20  # the mock's whole answer
    # The reservation used the route's cap of 64, not the 5,000 the caller asked for.
    assert tenant_spent() == cost_micro_usd(ALPHA, 7, 20)


@pytest.mark.parametrize("headers, payload, status, code", [
    ({}, None, 401, "invalid_api_key"),
    ({"Authorization": "Bearer sk-mock-alpha"}, None, 401, "invalid_api_key"),
    (GLOBEX, None, 403, "route_not_allowed"),             # globex's key may only classify
    (ACME, body(model="alpha-large"), 404, "unknown_route"),  # model IDs aren't route names
])
def test_keys_and_policy(gateway, mock_url, headers, payload, status, code):
    resp = ask(gateway().url, payload, headers=headers)
    assert (resp.status_code, resp.json()["error"]["code"]) == (status, code)
    assert calls(mock_url) == {"alpha": 0, "beta": 0}


def test_a_body_that_is_not_json_is_a_400(gateway):
    resp = http.post(f"{gateway().url}/v1/chat/completions", content=b"not json", headers=ACME)
    assert (resp.status_code, resp.json()["error"]["code"]) == (400, "invalid_json")


# ------------------------------------------------- errors: retry, fail over, stop

def test_503_is_retried_then_fails_over(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503")
    gw = gateway()
    resp = ask(gw.url)
    assert resp.status_code == 200
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert (resp.headers["x-llmgw-hop"], resp.headers["x-llmgw-attempts"]) == ("1", "4")
    assert calls(mock_url) == {"alpha": 3, "beta": 1}  # three tries, then the fallback
    assert tenant_spent() == cost_micro_usd(BETA, 7, 8)  # alpha's hold was released


def test_one_transient_503_is_absorbed_by_a_retry(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503", times=1)
    resp = ask(gateway().url)
    assert resp.headers["x-llmgw-target"] == "alpha/alpha-large"
    assert resp.headers["x-llmgw-attempts"] == "2"


def test_a_short_retry_after_is_waited_out(gateway, mock_url):
    control(mock_url, "alpha", "status", error="rate_limit_429", times=1)  # Retry-After: 1
    gw = gateway()
    started = time.perf_counter()
    resp = ask(gw.url)
    assert time.perf_counter() - started >= 1.0  # Retry-After is a minimum
    assert resp.headers["x-llmgw-target"] == "alpha/alpha-large"
    assert calls(mock_url) == {"alpha": 2, "beta": 0}


def test_slow_down_fails_over_and_opens_the_breaker_for_retry_after(gateway, mock_url):
    control(mock_url, "alpha", "status", error="slow_down_429")  # Retry-After: 5
    gw = gateway()
    started = time.perf_counter()
    resp = ask(gw.url)
    assert time.perf_counter() - started < 1.0       # nobody slept for 5 seconds
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert calls(mock_url)["alpha"] == 1          # no retry on the provider that said slow down
    assert 4 < rds.pttl("cb:{alpha/alpha-large}:open") / 1000 <= 5
    ask(gw.url)
    assert calls(mock_url) == {"alpha": 1, "beta": 2}  # the next request skips alpha


@pytest.mark.parametrize("error", ["anthropic_spend_cap_429", "anthropic_user_limit_400"])
def test_a_spend_limit_stops_the_provider_without_a_single_retry(gateway, mock_url, error):
    control(mock_url, "alpha", "status", error=error)
    resp = ask(gateway().url)
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert calls(mock_url) == {"alpha": 1, "beta": 1}
    assert rds.pttl("cb:{alpha/alpha-large}:open") / 1000 > STOP_OPEN_S - 5  # out for a long while


def test_a_refused_provider_key_fails_over_and_takes_the_provider_out(gateway, mock_url):
    from config import Provider
    providers = {"alpha": Provider(f"{mock_url}/alpha", "sk-rotated-away"),
                 "beta": Provider(f"{mock_url}/beta", "sk-mock-beta")}
    resp = ask(gateway(providers=providers).url)
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert calls(mock_url) == {"alpha": 1, "beta": 1}


def test_a_callers_bad_request_is_returned_as_is(gateway, mock_url):
    control(mock_url, "alpha", "status", error="bad_request_400")
    resp = ask(gateway().url)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_value"  # the provider's own body
    assert calls(mock_url) == {"alpha": 1, "beta": 0}       # no retry, no fallback
    assert tenant_spent() == 0


def test_a_hung_provider_costs_one_timeout_then_fails_over(gateway, mock_url):
    control(mock_url, "alpha", "hang")
    gw = gateway()
    started = time.perf_counter()
    resp = ask(gw.url)
    elapsed = time.perf_counter() - started
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert 1.0 <= elapsed < 1.6                        # the route's timeout_s, once
    assert calls(mock_url) == {"alpha": 1, "beta": 1}  # a timeout isn't retried in place


def test_all_providers_down_is_a_503_that_says_dont_retry(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503")
    control(mock_url, "beta", "status", error="overloaded_503")
    resp = ask(gateway(breaker=False).url)
    assert (resp.status_code, resp.json()["error"]["code"]) == (503, "all_routes_failed")
    assert resp.headers["x-should-retry"] == "false"
    assert "retry-after" not in resp.headers
    assert calls(mock_url) == {"alpha": 3, "beta": 3}
    assert tenant_spent() == 0


def test_with_the_breaker_the_503_carries_retry_after(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503")
    control(mock_url, "beta", "status", error="overloaded_503")
    resp = ask(gateway(breaker_open_s=7).url)
    assert resp.status_code == 503
    assert 6 <= int(resp.headers["retry-after"]) <= 7  # when the breaker will allow a probe
    assert "x-should-retry" not in resp.headers


def test_one_deadline_covers_every_attempt(gateway, mock_url):
    control(mock_url, "alpha", "hang")
    control(mock_url, "beta", "hang")
    gw = gateway(deadline_s=1.5)  # two targets x 1 s timeout would be 2 s
    started = time.perf_counter()
    resp = ask(gw.url)
    assert (resp.status_code, resp.json()["error"]["code"]) == (503, "all_routes_failed")
    assert 1.4 <= time.perf_counter() - started < 1.9
    assert "timeout" in resp.json()["error"]["message"]


def test_no_attempt_outlives_the_deadline(gateway, mock_url):
    control(mock_url, "alpha", "hang")
    control(mock_url, "beta", "ok", ttft_ms=500)  # healthy, but not in what is left
    gw = gateway(deadline_s=1.0)                   # alpha's timeout eats the whole second
    started = time.perf_counter()
    resp = ask(gw.url)
    assert resp.status_code == 503
    assert time.perf_counter() - started < 1.3     # not 1 s for alpha plus 0.5 s for beta


# ------------------------------------------------------------------- streaming

def test_a_stream_passes_through_and_settles_on_its_usage_chunk(gateway, mock_url):
    gw = gateway()
    resp, parsed = ask_stream(gw.url)
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert text_of(parsed) == "the quick brown fox jumps over the lazy "
    assert parsed[-1] == "[DONE]"
    assert all(event.get("choices") for event in parsed[:-1])  # no usage chunk: not asked for
    record, = gw.records
    assert record["stream"] is True and record["usage_estimated"] is False
    assert (record["input_tokens"], record["output_tokens"]) == (7, 8)
    assert record["ttft_s"] is not None
    assert tenant_spent() == 141


def test_the_usage_chunk_is_forwarded_when_the_caller_asks_for_it(gateway):
    _, parsed = ask_stream(gateway().url, body(stream_options={"include_usage": True}))
    assert parsed[-2]["usage"] == {"prompt_tokens": 7, "completion_tokens": 8, "total_tokens": 15}
    assert parsed[-2]["choices"] == []


def test_no_first_token_in_time_fails_over_and_the_caller_never_knows(gateway, mock_url):
    control(mock_url, "alpha", "hang")
    gw = gateway()
    started = time.perf_counter()
    resp, parsed = ask_stream(gw.url)
    elapsed = time.perf_counter() - started
    assert resp.status_code == 200 and resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert 0.3 <= elapsed < 0.9  # first_token_s, not the route's 1 s timeout
    assert text_of(parsed).startswith("the quick") and parsed[-1] == "[DONE]"


def test_a_role_chunk_is_not_a_first_token(gateway, mock_url):
    # alpha answers 200 and sends the role chunk at once, then goes silent.
    control(mock_url, "alpha", "stall_after", after=0)
    resp, parsed = ask_stream(gateway().url)
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    ids = {event["id"] for event in parsed if isinstance(event, dict)}
    assert len(ids) == 1  # nothing from alpha's half-open stream leaked to the caller


def test_without_the_first_token_timeout_a_stream_waits_the_whole_timeout(gateway, mock_url):
    control(mock_url, "alpha", "hang")
    gw = gateway(first_token_timeout=False)
    started = time.perf_counter()
    resp, _ = ask_stream(gw.url)
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert time.perf_counter() - started >= 1.0


def test_a_mid_stream_drop_is_reported_in_band_and_settled_on_what_was_sent(gateway, mock_url):
    control(mock_url, "alpha", "drop_after", after=3)
    gw = gateway()
    resp, parsed = ask_stream(gw.url)
    assert resp.status_code == 200                      # the 200 left with the first token
    assert resp.headers["x-llmgw-target"] == "alpha/alpha-large"
    assert text_of(parsed) == "the quick brown "        # three tokens made it
    assert parsed[-1]["error"]["code"] == "upstream_closed"
    assert "[DONE]" not in parsed                       # the stream did not finish
    assert calls(mock_url)["beta"] == 0                 # no fallback after the first token
    record, = gw.records
    assert record["status"] == "upstream_closed" and record["usage_estimated"] is True
    assert record["output_tokens"] == 3
    assert tenant_spent() == cost_micro_usd(ALPHA, 7, 3)  # not the 8 that were reserved
    assert rds.get("cb:{alpha/alpha-large}:fails") == "1"  # and it counts against alpha


def test_a_stream_that_goes_silent_hits_the_idle_gap_timeout(gateway, mock_url):
    control(mock_url, "alpha", "stall_after", after=2)
    gw = gateway()  # idle_gap_s is 0.4 in the tests
    started = time.perf_counter()
    _, parsed = ask_stream(gw.url)
    assert 0.4 <= time.perf_counter() - started < 1.0
    assert text_of(parsed) == "the quick "
    assert parsed[-1]["error"]["code"] == "idle_gap_timeout"
    assert gw.records[0]["output_tokens"] == 2


def test_a_caller_that_hangs_up_mid_stream_is_still_settled(gateway, mock_url):
    control(mock_url, "alpha", "ok", tpt_ms=100)  # a slow stream: 8 tokens over 0.7 s
    gw = gateway()
    payload = {**body(), "stream": True}
    with httpx.Client() as client:
        with client.stream("POST", f"{gw.url}/v1/chat/completions", json=payload,
                           headers=ACME) as resp:
            lines = resp.iter_lines()
            got = [next(lines) for _ in range(5)]  # role chunk, blank, 2 tokens, blank
    assert len(events(got)) >= 2
    deadline = time.perf_counter() + 3
    while not gw.records and time.perf_counter() < deadline:
        time.sleep(0.02)
    record, = gw.records  # written although the request was cancelled
    assert record["usage_estimated"] is True and 1 <= record["output_tokens"] < 8
    assert tenant_spent() == cost_micro_usd(ALPHA, 7, record["output_tokens"])
    assert rds.keys("budget:{demo}:resv:*") == []  # the reservation is gone, not left to expire


def test_the_openai_sdk_raises_on_the_in_band_error(gateway, mock_url):
    control(mock_url, "alpha", "drop_after", after=3)
    client = openai.OpenAI(base_url=f"{gateway().url}/v1", api_key="vk-demo-refunds-acme",
                           max_retries=0)
    received = []
    with pytest.raises(openai.APIError) as caught:
        stream = client.chat.completions.create(
            model="support-reply", max_tokens=8, stream=True,
            messages=[{"role": "user", "content": PROMPT}])
        for part in stream:
            received.append(part.choices[0].delta.content or "")
    assert "".join(received) == "the quick brown "
    assert "incomplete" in str(caught.value)
    client.close()


# --------------------------------------------------------------------- budgets

def test_an_exhausted_tenant_gets_a_429_before_any_provider_is_called(gateway, mock_url):
    rds.set(limit_key("demo", "tenant:acme"), 50)  # 50 micro-USD: less than one request
    resp = ask(gateway().url)
    assert (resp.status_code, resp.json()["error"]["code"]) == (429, "budget_exceeded")
    assert calls(mock_url) == {"alpha": 0, "beta": 0}
    assert tenant_spent() == 0


def test_a_nearly_empty_budget_steps_down_to_the_cheaper_target(gateway, mock_url):
    # alpha-large's worst case is 7*3 + 8*15 = 141; beta-large's is 7*2.5 + 8*10 = 98.
    rds.set(limit_key("demo", "tenant:acme"), 100)
    resp = ask(gateway().url)
    assert resp.status_code == 200
    assert resp.headers["x-llmgw-target"] == "beta/beta-large"
    assert calls(mock_url) == {"alpha": 0, "beta": 1}


def test_the_request_id_is_the_idempotency_key(gateway):
    resp = ask(gateway().url, **{"Idempotency-Key": "order-42-summary"})
    assert resp.headers["x-llmgw-request-id"] == "order-42-summary"


# ---------------------------------------------- what the caller's SDK does next

def test_x_should_retry_false_stops_the_openai_sdks_own_retries(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503")
    control(mock_url, "beta", "status", error="overloaded_503")
    gw = gateway(breaker=False)
    client = openai.OpenAI(base_url=f"{gw.url}/v1", api_key="vk-demo-refunds-acme")  # defaults
    assert client.max_retries == 2
    with pytest.raises(openai.InternalServerError):
        client.chat.completions.create(model="support-reply",
                                       messages=[{"role": "user", "content": PROMPT}])
    assert calls(mock_url) == {"alpha": 3, "beta": 3}  # one gateway request, not three
    client.close()


def test_concurrent_requests_do_not_share_state(gateway, mock_url):
    gw = gateway()
    results = []

    def one() -> None:
        with httpx.Client() as client:
            results.append(client.post(f"{gw.url}/v1/chat/completions", json=body(),
                                       headers=ACME).status_code)

    threads = [threading.Thread(target=one) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [200] * 20
    assert tenant_spent() == 20 * 141
    assert len(gw.records) == 20
