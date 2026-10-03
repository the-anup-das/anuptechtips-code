"""Usage records as OpenTelemetry spans, with the GenAI conventions' attribute names."""
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import telemetry
from helpers import ask, ask_stream, body, control


@pytest.fixture(scope="module")
def spans():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return exporter


def gateway_spans(exporter) -> list:
    """The gateway's own spans. FastAPI 0.142 adds server spans of its own once a tracer
    provider is set; those belong to the instrumentation scope "fastapi"."""
    return [span for span in exporter.get_finished_spans()
            if span.instrumentation_scope.name == "llm-gateway"]


def test_a_request_becomes_one_client_span(gateway, spans):
    spans.clear()
    gw = gateway()
    gw.records.clear()
    ask(gw.url)
    telemetry.emit(gw.records[0])  # the tests collect records; the gateway's default is emit
    span, = gateway_spans(spans)
    assert span.name == "chat alpha-large"  # {gen_ai.operation.name} {gen_ai.request.model}
    assert span.kind is trace.SpanKind.CLIENT
    assert span.end_time > span.start_time
    attrs = dict(span.attributes)
    assert attrs["gen_ai.operation.name"] == "chat"
    assert attrs["gen_ai.provider.name"] == "alpha"
    assert attrs["gen_ai.request.model"] == "alpha-large"
    assert (attrs["gen_ai.usage.input_tokens"], attrs["gen_ai.usage.output_tokens"]) == (7, 8)
    assert (attrs["llmgw.tenant"], attrs["llmgw.cost_micro_usd"]) == ("acme", 141)
    assert "gen_ai.request.stream" not in attrs  # set only for streams
    assert "error.type" not in attrs
    # Prompts and completions are opt-in in the conventions, and this gateway never opts in.
    assert not any(key.endswith(".messages") for key in attrs)


def test_a_broken_stream_carries_the_error_and_time_to_first_chunk(gateway, mock_url, spans):
    spans.clear()
    control(mock_url, "alpha", "drop_after", after=3)
    gw = gateway()
    ask_stream(gw.url)
    telemetry.emit(gw.records[0])
    attrs = dict(gateway_spans(spans)[0].attributes)
    assert attrs["gen_ai.request.stream"] is True
    assert attrs["gen_ai.response.time_to_first_chunk"] > 0
    assert attrs["error.type"] == "upstream_closed"
    assert attrs["gen_ai.usage.output_tokens"] == 3


def test_a_cache_hit_is_logged_but_makes_no_span(gateway, spans):
    spans.clear()
    gw = gateway()
    ask(gw.url, body("classify"))
    ask(gw.url, body("classify"))
    for record in gw.records:
        telemetry.emit(record)
    assert len(gateway_spans(spans)) == 1


def test_the_conventions_are_pinned_to_a_commit():
    assert len(telemetry.SEMCONV_COMMIT) == 40
