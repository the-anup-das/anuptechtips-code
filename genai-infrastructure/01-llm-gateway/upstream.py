"""One call to one provider: a plain completion, or a stream opened up to its first token.

Every timeout here is anyio.fail_after(), not asyncio.timeout(). With anyio 4.2, opening
a connection inside an asyncio.timeout() block leaves the task's cancel count one too
high, the timeout then surfaces as CancelledError instead of TimeoutError, and the caller
gets a 500. Starlette and httpx run on anyio, so anyio's own timeout is the one to use.
"""
import json
from collections.abc import AsyncIterator, Awaitable, Callable

import anyio
import httpx

from config import Provider
from errors import CONNECTION, FIRST_TOKEN_TIMEOUT, TIMEOUT, Verdict, classify
from routes import Target
from tokens import count_tokens


class UpstreamError(Exception):
    """An attempt failed before anything reached the caller, so the gateway is still
    free to retry it or to fail over."""

    def __init__(self, verdict: Verdict, status: int | None = None, body: object = None) -> None:
        super().__init__(verdict.reason)
        self.verdict, self.status, self.body = verdict, status, body


class Stream:
    """An open upstream stream, read up to and including its first token."""

    def __init__(self, resp: httpx.Response, events: AsyncIterator, held: list) -> None:
        self.resp, self.events, self.held = resp, events, held


def _request(client: httpx.AsyncClient, provider: Provider, target: Target, body: dict,
             max_tokens: int, stream: bool) -> httpx.Request:
    payload = {**body, "model": target.model, "max_tokens": max_tokens, "stream": stream}
    if stream:  # ask for the usage chunk, whatever the caller asked for
        payload["stream_options"] = {**(body.get("stream_options") or {}), "include_usage": True}
    else:
        payload.pop("stream_options", None)
    # The caller's virtual key stays here. The provider only ever sees the real one.
    return client.build_request("POST", f"{provider.base_url}/v1/chat/completions", json=payload,
                                headers={"Authorization": f"Bearer {provider.api_key}"})


def _error(resp: httpx.Response) -> UpstreamError:
    try:
        body = resp.json()
    except ValueError:
        body = {"error": {"message": resp.text[:200]}}
    return UpstreamError(classify(resp.status_code, resp.headers, body), resp.status_code, body)


async def complete(client: httpx.AsyncClient, provider: Provider, target: Target, body: dict,
                   max_tokens: int, timeout_s: float) -> httpx.Response:
    """One non-streamed attempt, start to finish, inside one timeout."""
    try:
        with anyio.fail_after(timeout_s):
            resp = await client.send(_request(client, provider, target, body, max_tokens, False))
    except TimeoutError:
        raise UpstreamError(TIMEOUT) from None
    except httpx.TransportError:
        raise UpstreamError(CONNECTION) from None
    if resp.status_code != 200:
        raise _error(resp)
    return resp


async def _events(resp: httpx.Response) -> AsyncIterator[tuple[str, dict | None]]:
    """Each SSE line, with its JSON payload when it has one."""
    async for line in resp.aiter_lines():
        payload = None
        if line.startswith("data: ") and line != "data: [DONE]":
            try:
                payload = json.loads(line[6:])
            except ValueError:
                pass
        yield line, payload


def has_token(payload: dict | None) -> bool:
    """True for a chunk that carries output: text or a tool call, not just the role."""
    for choice in (payload or {}).get("choices") or []:
        delta = choice.get("delta") or {}
        if delta.get("content") or delta.get("tool_calls"):
            return True
    return False


async def open_stream(client: httpx.AsyncClient, provider: Provider, target: Target, body: dict,
                      max_tokens: int, first_token_s: float) -> Stream:
    """Send the request and wait for the first token. Nothing has gone to the caller
    yet, so whatever fails in here can still fail over."""
    resp, opened = None, False
    try:
        with anyio.fail_after(first_token_s):
            request = _request(client, provider, target, body, max_tokens, True)
            resp = await client.send(request, stream=True)
            if resp.status_code != 200:
                await resp.aread()
                raise _error(resp)
            events, held = _events(resp), []
            async for line, payload in events:
                held.append((line, payload))  # the role chunk and pings wait here
                if has_token(payload):
                    opened = True
                    return Stream(resp, events, held)
            raise UpstreamError(CONNECTION)   # the stream ended without a single token
    except TimeoutError:
        raise UpstreamError(FIRST_TOKEN_TIMEOUT) from None
    except httpx.TransportError:
        raise UpstreamError(CONNECTION) from None
    finally:
        if resp is not None and not opened:
            await resp.aclose()


async def relay(stream: Stream, idle_gap_s: float, wants_usage: bool,
                done: Callable[[dict | None, int, str | None], Awaitable[None]]
                ) -> AsyncIterator[str]:
    """Forward the stream to the caller. After the first token a failure can't hide
    behind a fallback any more, so it is reported in-band and the stream ends."""
    usage, tokens_out, problem, finished = None, 0, None, False

    async def lines() -> AsyncIterator[tuple[str, dict | None]]:
        for item in stream.held:
            yield item
        while True:
            with anyio.fail_after(idle_gap_s):  # the longest silence between chunks
                item = await anext(stream.events, None)
            if item is None:
                return
            yield item

    try:
        try:
            async for line, payload in lines():
                if payload is not None:
                    if payload.get("usage"):
                        usage = payload["usage"]
                        if not wants_usage and not payload.get("choices"):
                            continue  # the usage chunk was for the gateway, not the caller
                    for choice in payload.get("choices") or []:
                        tokens_out += count_tokens((choice.get("delta") or {}).get("content") or "")
                finished = finished or line == "data: [DONE]"
                yield line + "\n"
            if not finished:
                problem = "upstream_closed"
        except TimeoutError:
            problem = "idle_gap_timeout"
        except httpx.TransportError:
            problem = "upstream_closed"
        if problem:
            error = {"type": "upstream_error", "code": problem,
                     "message": "the provider stopped mid-answer; what you have is incomplete"}
            yield f"data: {json.dumps({'error': error})}\n\n"
    finally:
        # A caller that hangs up cancels this generator. Settling must still happen.
        with anyio.CancelScope(shield=True):
            await stream.resp.aclose()
            await done(usage, tokens_out, problem)
