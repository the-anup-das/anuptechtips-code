"""Backpressure with a provider slowed to 5 seconds: who waits, who is turned away."""
import asyncio
import dataclasses
import time

import httpx
import pytest

from admit import Admission
from helpers import ACME, GLOBEX, ROUTES, body, calls, control, http
from routes import Route

SLOW = 5000  # ms: the mock's time to answer
# The test routes, but patient: a 30 s timeout, so a 5 s call is slow, not failed.
PATIENT = {alias: Route(tuple(dataclasses.replace(t, timeout_s=30) for t in route.chain))
           for alias, route in ROUTES.items()}


async def fire(url: str, n: int, payload: dict, headers: dict = ACME) -> list:
    """n requests at once. Returns (status, seconds, response headers) for each, or None
    for a request that was still waiting on the slow provider after 1.5 s."""
    async with httpx.AsyncClient(timeout=1.5) as client:
        async def one():
            started = time.perf_counter()
            try:
                resp = await client.post(f"{url}/v1/chat/completions", json=payload,
                                         headers=headers)
            except httpx.ReadTimeout:
                return None
            return resp.status_code, time.perf_counter() - started, resp.headers
        return await asyncio.gather(*(one() for _ in range(n)))


@pytest.mark.anyio
async def test_a_tenant_over_its_limit_gets_429_at_once(gateway, mock_url):
    control(mock_url, "alpha", "ok", ttft_ms=SLOW)
    gw = gateway(PATIENT, per_tenant=2)
    results = await fire(gw.url, 5, body("summarize"))
    rejected = [r for r in results if r is not None]
    assert len(rejected) == 3 and results.count(None) == 2  # two are in flight, three bounced
    for status, seconds, headers in rejected:
        assert status == 429 and seconds < 0.5              # no waiting behind the slow calls
        assert headers["retry-after"] == "1"
    assert calls(mock_url)["alpha"] == 2


@pytest.mark.anyio
async def test_one_tenants_limit_does_not_block_another(gateway, mock_url):
    control(mock_url, "beta", "ok", ttft_ms=SLOW)
    gw = gateway(PATIENT, per_tenant=1)
    acme, globex = await asyncio.gather(fire(gw.url, 1, body("classify")),
                                        fire(gw.url, 1, body("classify"), headers=GLOBEX))
    assert acme == [None] and globex == [None]  # both admitted; both waiting on the provider
    assert calls(mock_url)["beta"] == 2


@pytest.mark.anyio
async def test_a_saturated_provider_overflows_to_the_fallback(gateway, mock_url):
    control(mock_url, "alpha", "ok", ttft_ms=SLOW)
    gw = gateway(PATIENT, per_provider=2)
    results = await fire(gw.url, 6, body("support-reply"))
    served = [r for r in results if r is not None]
    assert results.count(None) == 2                      # two hold alpha's two slots
    assert len(served) == 4
    for status, seconds, headers in served:              # the rest waited 0.2 s, then moved on
        assert status == 200 and headers["x-llmgw-target"] == "beta/beta-large"
        assert seconds < 1.0
    stats = http.get(f"{mock_url}/_stats").json()
    assert stats["alpha"]["max_in_flight"] == 2          # the cap held


@pytest.mark.anyio
async def test_with_no_fallback_the_overflow_is_a_fast_503(gateway, mock_url):
    control(mock_url, "alpha", "ok", ttft_ms=SLOW)
    gw = gateway(PATIENT, per_provider=2)
    results = await fire(gw.url, 5, body("summarize"))   # summarize has only alpha
    shed = [r for r in results if r is not None]
    assert len(shed) == 3
    for status, seconds, headers in shed:
        assert status == 503 and seconds < 1.0           # not 5 s
        assert headers["retry-after"] == "1"             # overloaded, not broken: come back
    assert calls(mock_url)["alpha"] == 2


@pytest.mark.anyio
async def test_admit_waits_at_most_queue_wait_then_gives_up():
    admission = Admission(per_tenant=1, per_provider=1, queue_wait_s=0.1)
    assert await admission.admit("alpha") is True
    started = time.perf_counter()
    assert await admission.admit("alpha") is False
    # Windows timers can fire a few ms early, and a busy machine runs late.
    assert 0.09 <= time.perf_counter() - started < 1.0
    admission.release("alpha")
    assert await admission.admit("alpha") is True        # a freed slot is taken at once


@pytest.mark.anyio
async def test_a_full_queue_rejects_without_waiting():
    admission = Admission(per_tenant=1, per_provider=1, queue_wait_s=0.3, max_queue=2)
    assert await admission.admit("alpha") is True
    waiting = [asyncio.ensure_future(admission.admit("alpha")) for _ in range(2)]
    await asyncio.sleep(0.05)
    started = time.perf_counter()
    assert await admission.admit("alpha") is False       # third in line: no room to queue
    assert time.perf_counter() - started < 0.05
    assert await asyncio.gather(*waiting) == [False, False]


def test_tenant_slots_are_counted_per_tenant():
    admission = Admission(per_tenant=2, per_provider=1, queue_wait_s=0.1)
    assert admission.enter("acme") and admission.enter("acme")
    assert not admission.enter("acme")
    assert admission.enter("globex")
    admission.leave("acme")
    assert admission.enter("acme")
