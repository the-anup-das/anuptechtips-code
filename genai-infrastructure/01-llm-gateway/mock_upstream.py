"""A mock model provider with failure switches. One server plays every provider:
POST /{provider}/v1/chat/completions speaks the OpenAI chat-completions format,
streamed or not, and answers with a fixed sentence. No model, no network, no bill.

    uvicorn mock_upstream:app --port 8001

Set a provider's behaviour with POST /_control, for example
    {"provider": "alpha", "mode": "status", "error": "overloaded_503"}
    {"provider": "alpha", "mode": "drop_after", "after": 3}
Modes: ok, status (an error response), hang (never answers), stall_after (goes silent
after `after` tokens), drop_after (cuts the connection after `after` tokens).
`times` applies a mode to the next N calls only. `ttft_ms`, `tpt_ms` and `tokens` set
the time to first token, the time per token and the answer's length.
GET /_stats returns the call counts, POST /_reset clears everything.
"""
import asyncio
import itertools
import json
import os
import sys
import time
from dataclasses import dataclass, field

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from tokens import count_message_tokens

if sys.platform == "win32":
    # asyncio.sleep() rounds up to the 15.6 ms system tick on Windows unless the
    # process asks for a 1 ms timer. Without this, ttft_ms=5 would mean 15 ms.
    import ctypes
    ctypes.windll.winmm.timeBeginPeriod(1)

KEYS = {"alpha": "sk-mock-alpha", "beta": "sk-mock-beta"}
WORDS = "the quick brown fox jumps over the lazy dog".split()

# Error responses in the providers' own shapes (from their docs, checked 1 October 2026).
ERRORS: dict[str, tuple[int, dict, dict]] = {
    "overloaded_503": (503, {}, {"error": {
        "type": "service_unavailable_error", "code": "server_is_overloaded",
        "message": "The requested model is temporarily overloaded."}}),
    "rate_limit_429": (429, {"retry-after": "1"}, {"error": {
        "type": "rate_limit_error", "message": "You are sending requests too quickly."}}),
    "slow_down_429": (429, {"retry-after": "5"}, {"error": {
        "type": "rate_limit_error", "code": "slow_down",
        "message": "Your request rate increased too quickly."}}),
    "anthropic_spend_cap_429": (429, {}, {"type": "error", "error": {
        "type": "rate_limit_error",
        "message": "You have reached your API usage limits: your organization has crossed "
                   "its monthly API usage threshold.",
        "details": {"error_code": "enforced_spend_limit_reached"}}}),
    "anthropic_user_limit_400": (400, {}, {"type": "error", "error": {
        "type": "invalid_request_error",
        "message": "You have reached your specified API usage limits."}}),
    "anthropic_overloaded_529": (529, {}, {"type": "error", "error": {
        "type": "overloaded_error", "message": "Overloaded"}}),
    "bad_request_400": (400, {}, {"error": {
        "type": "invalid_request_error", "code": "invalid_value",
        "message": "Invalid value for 'temperature'."}}),
}


@dataclass
class Behaviour:
    mode: str = "ok"
    error: str = "overloaded_503"
    after: int = 0
    times: int | None = None  # None: until the next /_control call
    ttft_ms: float = float(os.environ.get("MOCK_TTFT_MS", "0"))
    tpt_ms: float = float(os.environ.get("MOCK_TPT_MS", "0"))
    tokens: int = int(os.environ.get("MOCK_TOKENS", "20"))


@dataclass
class ProviderState:
    behaviour: Behaviour = field(default_factory=Behaviour)
    calls: int = 0
    in_flight: int = 0
    max_in_flight: int = 0
    last_auth: str | None = None

    def next_behaviour(self) -> Behaviour:
        current = self.behaviour
        if current.times is not None:
            current.times -= 1
            if current.times <= 0:  # the mode has run its course: back to healthy
                self.behaviour = Behaviour(ttft_ms=current.ttft_ms, tpt_ms=current.tpt_ms,
                                           tokens=current.tokens)
        return current


class ConnectionDropped(Exception):
    """Raised mid-stream on purpose: the server then closes the socket without finishing."""


app = FastAPI()
state: dict[str, ProviderState] = {name: ProviderState() for name in KEYS}
sequence = itertools.count(1)


@app.post("/_control")
async def control(request: Request) -> dict:
    settings = await request.json()
    provider = state[settings.pop("provider")]
    provider.behaviour = Behaviour(**settings)
    return {"ok": True}


@app.get("/_stats")
async def stats() -> dict:
    return {name: {"calls": s.calls, "in_flight": s.in_flight,
                   "max_in_flight": s.max_in_flight, "last_auth": s.last_auth}
            for name, s in state.items()}


@app.post("/_reset")
async def reset() -> dict:
    for name in state:
        state[name] = ProviderState()
    return {"ok": True}


def chunk(request_id: str, model: str, delta: dict, finish: str | None = None) -> str:
    payload = {"id": request_id, "object": "chat.completion.chunk", "created": int(time.time()),
               "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    return f"data: {json.dumps(payload)}\n\n"


@app.post("/{provider}/v1/chat/completions")
async def chat_completions(provider: str, request: Request):
    body = await request.json()
    st = state[provider]
    st.calls += 1
    st.last_auth = request.headers.get("authorization")
    if st.last_auth != f"Bearer {KEYS[provider]}":
        return JSONResponse({"error": {"type": "authentication_error", "code": "invalid_api_key",
                                       "message": "Incorrect API key provided."}}, 401)
    b = st.next_behaviour()
    if b.mode == "status":
        status, headers, payload = ERRORS[b.error]
        return JSONResponse(payload, status, headers=headers)

    request_id = f"chatcmpl-mock-{next(sequence)}"
    model = body.get("model", "mock")
    n = min(int(body.get("max_tokens") or b.tokens), b.tokens)
    usage = {"prompt_tokens": count_message_tokens(body.get("messages", [])),
             "completion_tokens": n}
    usage["total_tokens"] = usage["prompt_tokens"] + n
    headers = {"x-request-id": request_id}
    st.in_flight += 1
    st.max_in_flight = max(st.max_in_flight, st.in_flight)

    if not body.get("stream"):
        try:
            if b.mode == "hang":
                await asyncio.sleep(3600)
            await asyncio.sleep((b.ttft_ms + b.tpt_ms * max(n - 1, 0)) / 1000)
        finally:
            st.in_flight -= 1
        text = " ".join(WORDS[i % len(WORDS)] for i in range(n))
        return JSONResponse({
            "id": request_id, "object": "chat.completion", "created": int(time.time()),
            "model": model, "usage": usage,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": text}}]}, headers=headers)

    async def sse():
        try:
            if b.mode == "hang":
                await asyncio.sleep(3600)
            yield chunk(request_id, model, {"role": "assistant", "content": ""})  # no token yet
            await asyncio.sleep(b.ttft_ms / 1000)
            for i in range(n):
                if i == b.after and b.mode == "stall_after":
                    await asyncio.sleep(3600)
                if i == b.after and b.mode == "drop_after":
                    raise ConnectionDropped(provider)
                if i:
                    await asyncio.sleep(b.tpt_ms / 1000)
                yield chunk(request_id, model, {"content": WORDS[i % len(WORDS)] + " "})
            yield chunk(request_id, model, {}, finish="stop")
            if (body.get("stream_options") or {}).get("include_usage"):
                final = {"id": request_id, "object": "chat.completion.chunk",
                         "created": int(time.time()), "model": model, "choices": [],
                         "usage": usage}
                yield f"data: {json.dumps(final)}\n\n"
            yield "data: [DONE]\n\n"
        finally:
            st.in_flight -= 1

    return StreamingResponse(sse(), media_type="text/event-stream", headers=headers)
