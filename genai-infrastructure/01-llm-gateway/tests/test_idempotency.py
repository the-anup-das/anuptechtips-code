"""Idempotency-Key at the gateway: a client's retry must not buy a second completion."""
import threading

import httpx
import redis

from budget import spent_key
from helpers import ACME, REDIS_URL, ask, ask_stream, body, calls, control

KEY = {"Idempotency-Key": "refund-8841-summary"}
rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def test_the_same_key_sent_ten_times_at_once_makes_one_upstream_call(gateway, mock_url):
    control(mock_url, "alpha", "ok", ttft_ms=300)  # slow enough for all ten to overlap
    gw = gateway()
    barrier, responses = threading.Barrier(10), []

    def send() -> None:
        with httpx.Client(timeout=30) as client:
            barrier.wait()
            responses.append(client.post(f"{gw.url}/v1/chat/completions", json=body(),
                                         headers={**ACME, **KEY}))

    threads = [threading.Thread(target=send) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200] + [409] * 9
    assert calls(mock_url) == {"alpha": 1, "beta": 0}
    in_flight = next(r for r in responses if r.status_code == 409)
    assert in_flight.json()["error"]["code"] == "idempotency_key_in_flight"
    assert int(rds.get(spent_key("demo", "tenant:acme"))) == 141  # charged once


def test_a_retry_after_completion_replays_the_stored_answer(gateway, mock_url):
    gw = gateway()
    first = ask(gw.url, **KEY)
    second = ask(gw.url, **KEY)
    assert second.status_code == 200 and second.json() == first.json()
    assert second.headers["idempotent-replayed"] == "true"
    assert "idempotent-replayed" not in first.headers
    assert calls(mock_url)["alpha"] == 1
    assert len(gw.records) == 1  # one usage record, one charge


def test_the_same_key_with_a_different_body_is_a_422(gateway, mock_url):
    gw = gateway()
    ask(gw.url, **KEY)
    resp = ask(gw.url, body(max_tokens=9), **KEY)
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "idempotency_key_reused")
    assert calls(mock_url)["alpha"] == 1


def test_keys_are_scoped_to_the_tenant(gateway, mock_url):
    from helpers import GLOBEX
    gw = gateway()
    ask(gw.url, body("classify", temperature=0.5), headers=ACME, **KEY)
    other = ask(gw.url, body("classify", temperature=0.9), headers=GLOBEX, **KEY)
    assert other.status_code == 200  # same key string, another tenant: no 422, no replay
    assert "idempotent-replayed" not in other.headers


def test_two_tenants_with_the_same_key_are_both_charged(gateway, mock_url):
    """acme and globex share an org. The same key string from both, at the same moment,
    must be two reservations: the reservation ID carries the tenant."""
    from helpers import GLOBEX, ROUTES
    from routes import cost_micro_usd
    control(mock_url, "beta", "ok", ttft_ms=300)  # slow enough for the two to overlap
    gw = gateway()
    barrier, responses = threading.Barrier(2), []

    def send(headers: dict) -> None:
        with httpx.Client(timeout=30) as client:
            barrier.wait()
            responses.append(client.post(f"{gw.url}/v1/chat/completions", json=body("classify"),
                                         headers={**headers, **KEY}))

    threads = [threading.Thread(target=send, args=(headers,)) for headers in (ACME, GLOBEX)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert [r.status_code for r in responses] == [200, 200]
    assert calls(mock_url)["beta"] == 2
    cost = cost_micro_usd(ROUTES["classify"].chain[0], 7, 8)
    assert int(rds.get(spent_key("demo", "tenant:acme"))) == cost
    assert int(rds.get(spent_key("demo", "tenant:globex"))) == cost
    assert int(rds.get(spent_key("demo", "org"))) == 2 * cost  # nobody rode on the other's hold


def test_a_failed_request_releases_the_key(gateway, mock_url):
    control(mock_url, "alpha", "status", error="overloaded_503")
    control(mock_url, "beta", "status", error="overloaded_503")
    gw = gateway(breaker=False)
    assert ask(gw.url, **KEY).status_code == 503
    control(mock_url, "alpha", "ok")
    again = ask(gw.url, **KEY)  # nothing was produced, so the same key may run again
    assert again.status_code == 200 and "idempotent-replayed" not in again.headers


def test_a_stream_cannot_be_replayed(gateway, mock_url):
    gw = gateway()
    headers = {**ACME, **KEY}
    ask_stream(gw.url, headers=headers)
    resp, _ = ask_stream(gw.url, headers=headers)
    assert resp.status_code == 409
    assert calls(mock_url)["alpha"] == 1  # the repeat did not buy a second stream
    plain = ask(gw.url, body(stream=True), **KEY)
    assert plain.json()["error"]["code"] == "stream_not_replayable"
