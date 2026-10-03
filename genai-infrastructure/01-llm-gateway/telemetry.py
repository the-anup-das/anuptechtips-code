"""One usage record per request, and the same facts as an OpenTelemetry span.

The span's attribute names follow OpenTelemetry's GenAI semantic conventions. Those are
still in "Development" and the repository has no release to pin, so the names here were
checked against one commit of open-telemetry/semantic-conventions-genai: SEMCONV_COMMIT.
"""
import json
import logging

try:
    from opentelemetry import trace
except ImportError:  # the gateway runs without OpenTelemetry installed
    trace = None

SEMCONV_COMMIT = "b31e9e8ea26ac1c086d3313d474e31d7c3f391ae"  # 30 September 2026

log = logging.getLogger("gateway.usage")


def span_attributes(record: dict) -> dict:
    attrs = {
        "gen_ai.operation.name": "chat",
        "gen_ai.provider.name": record["provider"],
        "gen_ai.request.model": record["model"],
        "gen_ai.request.max_tokens": record["max_tokens"],
        "gen_ai.usage.input_tokens": record["input_tokens"],
        "gen_ai.usage.output_tokens": record["output_tokens"],
        # Not in the conventions: who pays, and how the request travelled.
        "llmgw.route": record["route"],
        "llmgw.tenant": record["tenant"],
        "llmgw.team": record["team"],
        "llmgw.feature": record["feature"],
        "llmgw.fallback_hop": record["hop"],
        "llmgw.attempts": record["attempts"],
        "llmgw.cost_micro_usd": record["cost_micro_usd"],
        "llmgw.prices_version": record["prices_version"],
    }
    if record["stream"]:
        attrs["gen_ai.request.stream"] = True
        if record["ttft_s"] is not None:
            attrs["gen_ai.response.time_to_first_chunk"] = record["ttft_s"]
    if record["response_id"]:
        attrs["gen_ai.response.id"] = record["response_id"]
    if record["status"] != "ok":
        attrs["error.type"] = record["status"]
    return attrs


def emit(record: dict) -> None:
    """Write the usage record as one JSON log line, and as a span if a tracer is set up.
    Prompts and completions are left out on purpose: they are opt-in in the conventions."""
    log.info(json.dumps(record))
    if trace is None or record["cache"] == "hit":  # a cache hit called no model: no span
        return
    span = trace.get_tracer("llm-gateway").start_span(
        f"chat {record['model']}", kind=trace.SpanKind.CLIENT, start_time=record["start_ns"])
    span.set_attributes(span_attributes(record))
    span.end(end_time=record["end_ns"])
