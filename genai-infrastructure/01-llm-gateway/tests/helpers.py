"""Small helpers shared by the tests."""
import json
import os

import httpx

from routes import Route, Target

REDIS_URL = os.environ.get("LLMGW_REDIS_URL", "redis://localhost:56379/11")

ACME = {"Authorization": "Bearer vk-demo-refunds-acme"}    # tenant acme: every route
GLOBEX = {"Authorization": "Bearer vk-demo-search-globex"}  # tenant globex: classify only

# The production route table with short timeouts, so the failure tests run in seconds.
ROUTES = {
    "support-reply": Route((
        Target("alpha", "alpha-large", timeout_s=1.0, first_token_s=0.3, max_tokens=64),
        Target("beta", "beta-large", timeout_s=1.0, first_token_s=0.3, max_tokens=64),
    )),
    "classify": Route((
        Target("beta", "beta-small", timeout_s=1.0, first_token_s=0.3, max_tokens=16),
        Target("alpha", "alpha-small", timeout_s=1.0, first_token_s=0.3, max_tokens=16),
    ), cache_ttl_s=60),
    "summarize": Route((
        Target("alpha", "alpha-large", timeout_s=1.0, first_token_s=0.3, max_tokens=64),
    )),
}

PROMPT = "Summarize this refund request in one line"  # 7 tokens for the mock

http = httpx.Client(timeout=30)  # one client for the whole run: creating one is slow


def body(model: str = "support-reply", max_tokens: int = 8, **extra) -> dict:
    return {"model": model, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": PROMPT}], **extra}


def ask(url: str, payload: dict | None = None, headers: dict = ACME, **extra) -> httpx.Response:
    return http.post(f"{url}/v1/chat/completions", json=payload or body(),
                     headers={**headers, **extra})


def ask_stream(url: str, payload: dict | None = None, headers: dict = ACME
               ) -> tuple[httpx.Response, list[dict | str]]:
    """A streamed request, read to the end. Returns the response and its parsed events."""
    payload = {**(payload or body()), "stream": True}
    with http.stream("POST", f"{url}/v1/chat/completions", json=payload, headers=headers) as resp:
        lines = list(resp.iter_lines())
    return resp, events(lines)


def control(mock_url: str, provider: str, mode: str, **settings) -> None:
    http.post(f"{mock_url}/_control", json={"provider": provider, "mode": mode, **settings})


def calls(mock_url: str) -> dict[str, int]:
    return {name: s["calls"] for name, s in http.get(f"{mock_url}/_stats").json().items()}


def events(lines: list[str]) -> list[dict | str]:
    """The data payloads of an SSE stream, parsed ('[DONE]' stays a string)."""
    out = []
    for line in lines:
        if line.startswith("data: "):
            out.append("[DONE]" if line == "data: [DONE]" else json.loads(line[6:]))
    return out


def text_of(parsed: list[dict | str]) -> str:
    return "".join(choice["delta"].get("content") or ""
                   for event in parsed if isinstance(event, dict)
                   for choice in event.get("choices", []))
