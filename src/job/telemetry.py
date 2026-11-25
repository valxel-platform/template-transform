"""Traces for each run, and logs that point at them.

OpenTelemetry, configured from the standard environment variables, so OpenChoreo (or any
OpenTelemetry collector) needs nothing job-specific:

    OTEL_SERVICE_NAME                 e.g. emr-reminders
    OTEL_EXPORTER_OTLP_ENDPOINT       the data plane's collector, e.g. http://otel-collector:4318
    OTEL_RESOURCE_ATTRIBUTES          e.g. service.namespace=emr,deployment.environment.name=staging

A run is one trace: the `job.run` span (the job, its window, how it ended) with a span per call
the work makes to another service (httpx and requests are instrumented, so the call carries the
trace to the service it reaches) and per Postgres query. A job's process ends right after its run,
so `shutdown` exports what's left before it exits. Without an endpoint, spans are still created
(so log lines carry trace ids) but not exported.
"""

from __future__ import annotations

import contextlib
import os
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

_provider: TracerProvider | None = None


def setup(service: str, *, exporter: Any = None) -> TracerProvider:
    """Install the tracer provider once per process; `exporter` replaces OTLP (tests)."""
    global _provider
    if _provider is not None:
        if exporter is not None:
            _provider.add_span_processor(SimpleSpanProcessor(exporter))
        return _provider
    provider = TracerProvider(
        resource=Resource.create({"service.name": os.environ.get("OTEL_SERVICE_NAME") or service})
    )
    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    elif os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    for module, cls in (
        ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor"),
        ("opentelemetry.instrumentation.requests", "RequestsInstrumentor"),
        ("opentelemetry.instrumentation.psycopg", "PsycopgInstrumentor"),
    ):
        with contextlib.suppress(ImportError):  # instrument what the work uses
            getattr(__import__(module, fromlist=[cls]), cls)().instrument()
    _provider = provider
    return provider


def tracer() -> trace.Tracer:
    return trace.get_tracer("job")


def trace_ids() -> tuple[str, str]:
    ctx = trace.get_current_span().get_span_context()
    return (f"{ctx.trace_id:032x}", f"{ctx.span_id:016x}") if ctx.is_valid else ("", "")


def shutdown() -> None:
    """Export the spans still queued: the process ends with the run."""
    if _provider is not None:
        _provider.force_flush(5000)
