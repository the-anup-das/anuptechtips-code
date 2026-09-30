"""The FastAPI endpoint: header handling, status codes and the replay marker."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI, Header
from fastapi.testclient import TestClient

import app as app_module
from helpers import charges

BODY = {"amount": 1000, "currency": "usd"}


@pytest.fixture
def api(pg):
    with TestClient(app_module.app, raise_server_exceptions=False) as client:
        yield client


def post(api, key=None, client="c1", body=BODY):
    headers = {"X-Client-Id": client}
    if key is not None:
        headers["Idempotency-Key"] = key
    return api.post("/charges", json=body, headers=headers)


def test_missing_key_is_400(api, pg):
    resp = post(api)
    assert resp.status_code == 400
    assert charges(pg, "c1") == 0


def test_a_key_longer_than_255_characters_is_400(api):
    assert post(api, "x" * 256).status_code == 400
    assert post(api, '""').status_code == 400


def test_new_key_creates_one_charge(api, pg):
    resp = post(api, "key-1")
    assert resp.status_code == 201
    assert "Idempotent-Replayed" not in resp.headers
    assert charges(pg, "c1") == 1


def test_retry_gets_the_original_charge_back(api, pg):
    first = post(api, "key-1")
    retry = post(api, "key-1")
    assert retry.status_code == 201 and retry.json() == first.json()
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert charges(pg, "c1") == 1


def test_a_quoted_key_is_the_same_key(api, pg):
    first = post(api, "key-1")                 # Stripe style: bare
    retry = post(api, '"key-1"')               # IETF draft style: a quoted sf-string
    assert retry.headers.get("Idempotent-Replayed") == "true"
    assert retry.json() == first.json()


def test_same_key_with_a_different_body_is_422(api, pg):
    post(api, "key-1")
    resp = post(api, "key-1", body={"amount": 5000, "currency": "usd"})
    assert resp.status_code == 422
    assert charges(pg, "c1") == 1


def test_default_fields_count_as_the_same_request(api, pg):
    post(api, "key-1", body={"amount": 1000})  # currency defaults to usd
    assert post(api, "key-1", body=BODY).headers.get("Idempotent-Replayed") == "true"


def test_keys_are_scoped_per_client(api, pg):
    assert post(api, "same-key", client="alice").status_code == 201
    resp = post(api, "same-key", client="bob")
    assert resp.status_code == 201 and "Idempotent-Replayed" not in resp.headers
    assert charges(pg, "alice") == 1 and charges(pg, "bob") == 1


def test_a_crash_inside_the_charge_is_a_500_and_the_retry_runs(api, pg, monkeypatch):
    real_charge = app_module.charge
    calls = {"n": 0}

    def flaky(conn, *args):
        calls["n"] += 1
        result = real_charge(conn, *args)
        if calls["n"] == 1:
            raise RuntimeError("card network timeout")  # after the INSERT: must roll back
        return result

    monkeypatch.setattr(app_module, "charge", flaky)
    assert post(api, "key-1").status_code == 500
    assert charges(pg, "c1") == 0  # the first attempt's charge rolled back
    assert post(api, "key-1").status_code == 201
    assert charges(pg, "c1") == 1


def test_concurrent_duplicates_get_409_and_charge_once(api, pg):
    with ThreadPoolExecutor(max_workers=10) as pool:
        codes = sorted(pool.map(lambda _: post(api, "key-1").status_code, range(10)))
    assert charges(pg, "c1") == 1
    assert set(codes) <= {201, 409} and 409 in codes


def test_why_the_header_is_optional_in_the_signature():
    """With a required Header(), FastAPI answers a missing key with its own 422,
    which a client would read as 'reused with a different payload'."""
    strict = FastAPI()

    @strict.post("/x")
    def x(idempotency_key: str = Header()):
        return {}

    assert TestClient(strict).post("/x").status_code == 422
