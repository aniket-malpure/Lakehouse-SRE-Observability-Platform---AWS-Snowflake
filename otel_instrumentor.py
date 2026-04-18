"""
otel_instrumentor.py
--------------------
OpenTelemetry instrumentation for Lakehouse pipeline functions.
Wraps pipeline steps with distributed traces and span attributes,
enabling end-to-end latency tracking and error attribution.

Usage:
    from otel_instrumentor import tracer, instrument_pipeline

    @instrument_pipeline("RAW_INGEST_PIPELINE")
    def run_raw_ingest():
        ...
"""

import functools
import time
from typing import Callable, Optional

import yaml
from loguru import logger
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode


# ─────────────────────────────────────────────────────────────────────────────
# Tracer Setup
# ─────────────────────────────────────────────────────────────────────────────

def _build_tracer_provider(config_path: str = "config/config.yaml") -> TracerProvider:
    with open(config_path) as f:
        config = yaml.safe_load(f)

    otel_cfg = config.get("otel", {})
    service_name = otel_cfg.get("service_name", "lakehouse-sre-platform")
    otlp_endpoint = otel_cfg.get("otlp_endpoint", "http://localhost:4317")

    resource = Resource.create({
        "service.name": service_name,
        "deployment.environment": "production",
        "cloud.provider": "aws",
    })

    provider = TracerProvider(resource=resource)

    # OTLP exporter — sends to Grafana Tempo or Jaeger
    try:
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        logger.info(f"OTLP trace exporter configured: {otlp_endpoint}")
    except Exception as e:
        logger.warning(f"OTLP exporter unavailable ({e}), falling back to console.")
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    return provider


_provider = _build_tracer_provider()
tracer = trace.get_tracer("lakehouse.sre.instrumentor")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Span Context
# ─────────────────────────────────────────────────────────────────────────────

class PipelineSpan:
    """
    Context manager that wraps a pipeline step in an OTel span.
    Records attributes, row counts, latency, and error status.
    """

    def __init__(
        self,
        pipeline_name: str,
        step_name: str,
        target_table: Optional[str] = None,
        source: Optional[str] = None,
    ):
        self.pipeline_name = pipeline_name
        self.step_name = step_name
        self.target_table = target_table
        self.source = source
        self._span = None
        self._start_time = None

    def __enter__(self):
        self._start_time = time.perf_counter()
        self._span = tracer.start_span(
            name=f"{self.pipeline_name}.{self.step_name}",
            kind=SpanKind.INTERNAL,
        )
        self._span.set_attribute("pipeline.name", self.pipeline_name)
        self._span.set_attribute("pipeline.step", self.step_name)

        if self.target_table:
            self._span.set_attribute("db.table", self.target_table)
        if self.source:
            self._span.set_attribute("pipeline.source", self.source)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self._start_time

        self._span.set_attribute("pipeline.duration_seconds", round(elapsed, 3))

        if exc_type:
            self._span.set_status(
                Status(StatusCode.ERROR, description=str(exc_val))
            )
            self._span.record_exception(exc_val)
            logger.error(
                f"Span {self.pipeline_name}.{self.step_name} "
                f"failed after {elapsed:.2f}s: {exc_val}"
            )
        else:
            self._span.set_status(Status(StatusCode.OK))

        self._span.end()
        return False  # don't suppress exceptions

    def set_row_count(self, rows_in: int = 0, rows_out: int = 0) -> None:
        """Record row counts on the active span."""
        if self._span:
            self._span.set_attribute("pipeline.rows_in", rows_in)
            self._span.set_attribute("pipeline.rows_out", rows_out)

    def set_custom_attribute(self, key: str, value) -> None:
        if self._span:
            self._span.set_attribute(key, value)


# ─────────────────────────────────────────────────────────────────────────────
# Decorator
# ─────────────────────────────────────────────────────────────────────────────

def instrument_pipeline(
    pipeline_name: str,
    step_name: Optional[str] = None,
    target_table: Optional[str] = None,
):
    """
    Decorator that wraps any pipeline function with an OpenTelemetry span.

    Args:
        pipeline_name:  Logical name of the pipeline (e.g. "RAW_INGEST_PIPELINE")
        step_name:      Step name; defaults to function name
        target_table:   Target Snowflake / Iceberg table for this step

    Example:
        @instrument_pipeline("RAW_INGEST_PIPELINE", target_table="raw.events")
        def ingest_raw_events(rows):
            ...
    """

    def decorator(func: Callable) -> Callable:
        resolved_step = step_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with PipelineSpan(
                pipeline_name=pipeline_name,
                step_name=resolved_step,
                target_table=target_table,
            ) as span:
                result = func(*args, span=span, **kwargs)
            return result

        return wrapper

    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# Instrumented Step Examples (used in pipeline_monitor.py)
# ─────────────────────────────────────────────────────────────────────────────

@instrument_pipeline("RAW_INGEST_PIPELINE", target_table="raw.pipeline_metrics")
def trace_raw_ingest_step(data: list, span: PipelineSpan = None):
    """Example instrumented step — raw ingest."""
    rows_in = len(data)
    # ... actual ingest logic here ...
    rows_out = rows_in  # assume 1:1 for raw

    if span:
        span.set_row_count(rows_in=rows_in, rows_out=rows_out)
        span.set_custom_attribute("ingest.source", "snowflake_query_history")

    return data


@instrument_pipeline("CURATED_TRANSFORM_PIPELINE", target_table="curated.slo_burn_rate")
def trace_curated_transform_step(data: list, span: PipelineSpan = None):
    """Example instrumented step — curated transform."""
    # ... actual transform logic ...
    if span:
        span.set_row_count(rows_in=len(data), rows_out=len(data))
    return data
