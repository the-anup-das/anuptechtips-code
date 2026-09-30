"""The client: one key per operation, reused on every attempt (httpx MockTransport)."""
import json
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

import app as app_module
from client import key_for, new_key, post_with_retries
from helpers import charges

BODY = {"amount": 1000, "currency": "usd"}


def scripted(*outcomes):
    """A MockTransport that plays back outcomes (an int status or an exception) in order."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        outcome = outcomes[len(seen) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome, json={})

    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://api"), seen


def test_every_attempt_carries_the_same_key_and_body():
    http, seen = scripted(httpx.ReadTimeout("timed out"), 409, 201)
    key = new_key()
    resp = post_with_retries(http, "/charges", BODY, key, base_delay=0)
    assert resp.status_code == 201 and len(seen) == 3
    assert {req.headers["Idempotency-Key"] for req in seen} == {key}
    assert {req.content for req in seen} == {seen[0].content}


def test_a_client_error_is_not_retried():
    http, seen = scripted(422)
    assert post_with_retries(http, "/charges", BODY, new_key(), base_delay=0).status_code == 422
    assert len(seen) == 1


def test_it_gives_up_after_the_last_attempt():
    http, seen = scripted(503, 503, 503)
    assert post_with_retries(http, "/charges", BODY, new_key(), attempts=3,
                             base_delay=0).status_code == 503
    http, seen = scripted(*[httpx.ConnectError("refused")] * 3)
    with pytest.raises(httpx.ConnectError):
        post_with_retries(http, "/charges", BODY, new_key(), attempts=3, base_delay=0)
    assert len(seen) == 3


def test_new_key_is_a_v4_uuid_and_key_for_is_deterministic():
    assert uuid.UUID(new_key()).version == 4
    assert key_for("charge", "order-42") == key_for("charge", "order-42")
    assert key_for("charge", "order-42") != key_for("charge", "order-43")
    assert key_for("refund", "order-42") != key_for("charge", "order-42")


def test_a_lost_response_is_recovered_by_the_retry(pg):
    """End to end: the server charges the card, the response is lost on the way back,
    and the retry with the same key gets the original charge instead of a new one."""
    server = TestClient(app_module.app)
    delivered = []

    def lossy_network(request: httpx.Request) -> httpx.Response:
        real = server.post(request.url.path, content=request.content, headers=request.headers)
        if not delivered:
            delivered.append(real.json())  # what the server sent...
            raise httpx.ReadTimeout("response lost", request=request)  # ...and never arrived
        return httpx.Response(real.status_code, headers=real.headers, content=real.content)

    http = httpx.Client(transport=httpx.MockTransport(lossy_network), base_url="http://api",
                        headers={"X-Client-Id": "c1"})
    resp = post_with_retries(http, "/charges", BODY, new_key(), base_delay=0)
    assert resp.status_code == 201
    assert resp.headers["Idempotent-Replayed"] == "true"
    assert resp.json() == delivered[0]
    assert charges(pg, "c1") == 1
