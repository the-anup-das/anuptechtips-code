"""A minimal LLM gateway: one OpenAI-compatible endpoint in front of several providers.

    uvicorn gateway:app --port 8000

The request path: authenticate, apply policy, check the cache, walk the route (reserve
budget, check the circuit breaker, call the provider with timeouts and retries, fail
over), stream back, then settle and record. Each concern has its own module; this file
is the path that joins them.
"""
import asyncio
import random
import time
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
import redis.asyncio as redis
from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse

import telemetry
from admit import Admission
from breaker import Breaker, Gate, NoBreaker
from budget import Budget, NoBudget, Reservation
from cache import Cache, cache_key
from config import Caller, Settings, key_hash
from errors import Action
from idempotency import Claim, Conflict, Replay, claim
from naive_budget import NaiveBudget
from routes import PRICES_VERSION, ROUTES, Route, Target, cost_micro_usd
from tokens import count_message_tokens
from upstream import Stream, UpstreamError, complete, open_stream, relay

STOP_OPEN_S = 600.0  # a provider that is out of money or refuses our key sits out this long


class GatewayError(Exception):
    """An error the gateway answers with itself, in the OpenAI error shape."""

    def __init__(self, status: int, code: str, message: str, retry_after: float | None = None,
                 body: object = None) -> None:
        self.status, self.code, self.message = status, code, message
        self.retry_after, self.body = retry_after, body


@dataclass
class Gateway:
    settings: Settings
    routes: dict[str, Route]
    on_usage: Callable[[dict], None]
    redis: redis.Redis
    http: httpx.AsyncClient
    budget: Budget | NaiveBudget | NoBudget
    breaker: Breaker | NoBreaker
    cache: Cache
    admission: Admission


@dataclass
class Call:
    """One request on its way through the gateway."""
    caller: Caller
    alias: str        # the route the caller asked for, e.g. "support-reply"
    request_id: str   # the Idempotency-Key when there is one; with the tenant, the reservation ID
    started: float    # time.perf_counter(): monotonic, and finer than time.monotonic()
    start_ns: int     # time.time_ns(), for the trace


@dataclass
class Served:
    """A target that answered, and what it took to get there."""
    target: Target
    hop: int                         # 0 = the primary, 1 = the first fallback, ...
    attempts: int                    # upstream calls made for this request, failures included
    resv: Reservation
    max_tokens: int
    tokens_in: int
    result: httpx.Response | Stream  # a finished completion, or a stream past its first token


def backoff(s: Settings, attempt: int) -> float:
    """Exponential backoff with full jitter."""
    return random.uniform(0, min(s.backoff_cap_s, s.backoff_base_s * 2 ** attempt))


async def try_target(gw: Gateway, target: Target, body: dict, max_tokens: int,
                     deadline: float, failures: list[str]) -> tuple[object, int]:
    """Call one target, retrying only what is worth retrying. Returns (result, calls
    made); the result is None when it is time to move down the chain."""
    s = gw.settings
    provider = s.providers[target.provider]
    for attempt in range(s.attempts):
        gate = await gw.breaker.allow(target.name)
        left = deadline - time.perf_counter()
        if gate.state == "open" or left <= 0:
            failures.append(f"{target.name}: " + ("circuit open" if left > 0 else "out of time"))
            return None, attempt
        try:
            if body.get("stream"):
                wait_s = target.first_token_s if s.first_token_timeout else target.timeout_s
                result = await open_stream(gw.http, provider, target, body, max_tokens,
                                           min(wait_s, left))
            else:
                result = await complete(gw.http, provider, target, body, max_tokens,
                                        min(target.timeout_s, left))
        except UpstreamError as err:
            verdict = err.verdict
            if verdict.action is Action.FAIL:  # the caller's own mistake: no retry, no fallback
                raise GatewayError(err.status, verdict.reason, "rejected by the provider",
                                   body=err.body) from None
            # Retry-After is a minimum. The jittered backoff goes on top of it.
            wait = (verdict.retry_after or 0) + backoff(s, attempt)
            too_long = (verdict.retry_after or 0) > s.max_wait_s
            open_for = STOP_OPEN_S if verdict.action is Action.STOP else None
            if too_long:  # the provider said when to come back: stay away until then
                open_for = verdict.retry_after
            await gw.breaker.failure(target.name, gate, open_for)
            retry = (verdict.action is Action.RETRY and not too_long and gate.state != "probe"
                     and attempt + 1 < s.attempts and time.perf_counter() + wait < deadline)
            if not retry:
                failures.append(f"{target.name}: {verdict.reason}")
                return None, attempt + 1
            await asyncio.sleep(wait)
        else:
            await gw.breaker.success(target.name, gate)
            return result, attempt + 1
    return None, s.attempts


async def call_chain(gw: Gateway, call: Call, route: Route, body: dict) -> Served:
    """Walk the route's chain until a target answers. All of this happens before the
    first byte reaches the caller, which is why failing over is still invisible."""
    deadline = call.started + gw.settings.deadline_s  # one deadline for every attempt
    tokens_in = count_message_tokens(body.get("messages") or [])
    caller, failures, calls, over_budget, saturated = call.caller, [], 0, 0, 0
    # Idempotency keys are per tenant, so the reservation ID is too: two tenants of one
    # org that send the same key must not share one hold.
    resv_id = f"{caller.tenant}:{call.request_id}"
    for hop, target in enumerate(route.chain):
        max_tokens = min(int(body.get("max_tokens") or target.max_tokens), target.max_tokens)
        worst = cost_micro_usd(target, tokens_in, max_tokens)  # input + every token allowed
        resv = await gw.budget.reserve(caller.org, caller.levels(), resv_id, worst)
        if resv is None:
            failures.append(f"{target.name}: over budget")  # a cheaper target may still fit
            over_budget += 1
            continue
        if not await gw.admission.admit(target.provider):
            await gw.budget.settle(resv, 0)
            failures.append(f"{target.name}: saturated")
            saturated += 1
            continue
        try:
            result, made = await try_target(gw, target, body, max_tokens, deadline, failures)
        except BaseException:
            gw.admission.release(target.provider)
            await gw.budget.settle(resv, 0)
            raise
        calls += made
        if result is not None:
            return Served(target, hop, calls, resv, max_tokens, tokens_in, result)
        gw.admission.release(target.provider)
        await gw.budget.settle(resv, 0)  # nothing was generated here: release the hold

    if over_budget == len(route.chain):  # the tenant's own limit: 429, and retrying won't help
        raise GatewayError(429, "budget_exceeded", "; ".join(failures))
    if saturated and saturated + over_budget == len(route.chain):  # ours: 503, come back soon
        raise GatewayError(503, "gateway_overloaded", "; ".join(failures), retry_after=1)
    waits = [await gw.breaker.retry_after(target.name) for target in route.chain]
    raise GatewayError(503, "all_routes_failed", "; ".join(failures),
                       retry_after=min((w for w in waits if w > 0), default=None))


async def finish(gw: Gateway, call: Call, served: Served, usage: dict | None, tokens_out: int,
                 problem: str | None, ttft_s: float | None, response_id: str | None) -> int:
    """Settle the budget on what was really used, free the slots, write the usage record."""
    target, caller = served.target, call.caller
    try:
        tokens_in = usage["prompt_tokens"] if usage else served.tokens_in
        tokens_out = usage["completion_tokens"] if usage else tokens_out
        cost = cost_micro_usd(target, tokens_in, tokens_out)
        await gw.budget.settle(served.resv, cost)
        if problem:  # the stream broke after its first token: that still counts against the provider
            await gw.breaker.failure(target.name, Gate("closed", 0))
    finally:
        gw.admission.release(target.provider)
        gw.admission.leave(caller.tenant)
    gw.on_usage({
        "request_id": call.request_id, "team": caller.team, "feature": caller.feature,
        "tenant": caller.tenant, "key_id": caller.key_id, "route": call.alias, "cache": "miss",
        "provider": target.provider, "model": target.model, "hop": served.hop,
        "attempts": served.attempts, "stream": isinstance(served.result, Stream),
        "max_tokens": served.max_tokens, "input_tokens": tokens_in, "output_tokens": tokens_out,
        "usage_estimated": usage is None,  # the stream ended before its usage chunk arrived
        "cost_micro_usd": cost, "prices_version": PRICES_VERSION, "status": problem or "ok",
        "ttft_s": ttft_s, "latency_s": round(time.perf_counter() - call.started, 4),
        "response_id": response_id, "start_ns": call.start_ns, "end_ns": time.time_ns(),
    })
    return cost


async def parsed_body(request: Request) -> dict:
    try:
        body = await request.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise GatewayError(400, "invalid_json", "the request body must be a JSON object")
    return body


async def authenticate(request: Request, authorization: str | None = Header(default=None)) -> Caller:
    """The virtual key says who is calling. A provider's own key is never accepted here."""
    token = (authorization or "").removeprefix("Bearer ").strip()
    caller = request.app.state.gw.settings.keys.get(key_hash(token))
    if caller is None:
        raise GatewayError(401, "invalid_api_key", "unknown virtual key")
    return caller


async def allowed_route(request: Request, body: dict = Depends(parsed_body),
                        caller: Caller = Depends(authenticate)) -> Route:
    """Policy: callers name a task, not a model, and only the tasks their key allows."""
    alias = body.get("model")
    route = request.app.state.gw.routes.get(alias)
    if route is None:
        raise GatewayError(404, "unknown_route", f"no route named {alias!r}")
    if alias not in caller.routes:
        raise GatewayError(403, "route_not_allowed", f"this key may not call {alias!r}")
    return route


async def idempotent(request: Request, body: dict = Depends(parsed_body),
                     caller: Caller = Depends(authenticate),
                     route: Route = Depends(allowed_route),
                     idempotency_key: str | None = Header(default=None)):
    """Claim the caller's Idempotency-Key, if it sent one. A repeat of a finished request
    gets the stored answer (Replay); a repeat that can't be served is a Conflict."""
    if idempotency_key is None:
        yield None
        return
    claimed = await claim(request.app.state.gw.redis, caller.tenant, idempotency_key, body)
    try:
        yield claimed
    except Exception:
        await claimed.release()  # the request failed: the same key may be tried again
        raise


def create_app(settings: Settings | None = None, routes: dict[str, Route] = ROUTES,
               on_usage: Callable[[dict], None] = telemetry.emit) -> FastAPI:
    s = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # redis-py's default async pool raises MaxConnectionsError at 100 connections in
        # use. A blocking pool makes a burst wait its turn for a connection instead.
        r = redis.Redis(connection_pool=redis.BlockingConnectionPool.from_url(
            s.redis_url, decode_responses=True, max_connections=s.redis_pool, timeout=2))
        # The HTTP pool is as big as the admission caps, so no call queues unseen in httpx.
        pool = s.per_provider * len(s.providers)
        http = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=2.0),
                                 limits=httpx.Limits(max_connections=pool,
                                                     max_keepalive_connections=pool))
        budgets = {"lua": Budget, "naive": NaiveBudget}
        app.state.gw = Gateway(
            s, routes, on_usage, r, http,
            budget=budgets[s.budget](r) if s.budget in budgets else NoBudget(),
            breaker=Breaker(r, open_s=s.breaker_open_s) if s.breaker else NoBreaker(),
            cache=Cache(r), admission=Admission(s.per_tenant, s.per_provider, s.queue_wait_s))
        yield
        await http.aclose()
        await r.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def gateway_error(request: Request, exc: GatewayError) -> JSONResponse:
        headers = {}
        if exc.retry_after is not None:
            headers["Retry-After"] = str(max(1, round(exc.retry_after)))
        elif s.retry_hint:
            # The gateway has already retried upstream. This header tells the OpenAI and
            # Anthropic SDKs not to stack their own retries on top of that.
            headers["x-should-retry"] = "false"
        if exc.body is not None:  # a provider's own 4xx, passed through as it came
            return JSONResponse(exc.body, exc.status, headers=headers)
        error = {"type": "gateway_error", "code": exc.code, "message": exc.message}
        return JSONResponse({"error": error}, exc.status, headers=headers)

    @app.exception_handler(Conflict)
    async def conflict(request: Request, exc: Conflict) -> JSONResponse:
        error = {"type": "idempotency_error", "code": exc.code, "message": exc.message}
        return JSONResponse({"error": error}, exc.status, headers={"x-should-retry": "false"})

    @app.exception_handler(Replay)
    async def replay(request: Request, exc: Replay) -> JSONResponse:
        return JSONResponse(exc.body, exc.status, headers={"Idempotent-Replayed": "true"})

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request, body: dict = Depends(parsed_body),
                               caller: Caller = Depends(authenticate),
                               route: Route = Depends(allowed_route),
                               claimed: Claim | None = Depends(idempotent)):
        gw: Gateway = request.app.state.gw
        stream = bool(body.get("stream"))
        call = Call(caller, body["model"],
                    request.headers.get("idempotency-key") or uuid.uuid4().hex,
                    time.perf_counter(), time.time_ns())

        if not gw.admission.enter(caller.tenant):
            raise GatewayError(429, "tenant_concurrency_exceeded",
                               f"more than {s.per_tenant} requests in flight", retry_after=1)
        try:
            ckey = cache_key(caller.tenant, route, body) if route.cache_ttl_s and not stream else None
            hit = await gw.cache.get(ckey) if ckey else None
            served = await call_chain(gw, call, route, body) if hit is None else None
        except BaseException:
            gw.admission.leave(caller.tenant)
            raise

        if served is None:  # answered from the cache: no provider, no reservation, no cost
            gw.admission.leave(caller.tenant)
            gw.on_usage({"request_id": call.request_id, "team": caller.team,
                         "feature": caller.feature, "tenant": caller.tenant,
                         "key_id": caller.key_id, "route": call.alias, "cache": "hit",
                         "cost_micro_usd": 0, "status": "ok",
                         "latency_s": round(time.perf_counter() - call.started, 4)})
            if claimed:
                await claimed.complete(200, hit)
            return JSONResponse(hit, headers={"x-llmgw-cache": "hit"})

        headers = {"x-llmgw-target": served.target.name, "x-llmgw-hop": str(served.hop),
                   "x-llmgw-attempts": str(served.attempts), "x-llmgw-request-id": call.request_id}
        if isinstance(served.result, Stream):
            ttft_s = round(time.perf_counter() - call.started, 4)
            wants_usage = bool((body.get("stream_options") or {}).get("include_usage"))
            response_id = served.result.resp.headers.get("x-request-id")

            async def done(usage: dict | None, tokens_out: int, problem: str | None) -> None:
                await finish(gw, call, served, usage, tokens_out, problem, ttft_s, response_id)

            if claimed:
                await claimed.streamed()
            return StreamingResponse(relay(served.result, s.idle_gap_s, wants_usage, done),
                                     media_type="text/event-stream", headers=headers)

        payload = served.result.json()
        cost = await finish(gw, call, served, payload.get("usage"), 0, None, None,
                            served.result.headers.get("x-request-id"))
        headers["x-llmgw-cost-micro-usd"] = str(cost)
        if ckey:
            await gw.cache.put(ckey, payload, route.cache_ttl_s)
        if claimed:
            await claimed.complete(200, payload)
        return JSONResponse(payload, headers=headers)

    return app


app = create_app()
