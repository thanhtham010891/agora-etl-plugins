"""Optional OpenTelemetry helpers for Kafka plugins."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

import logstruct

logger = logstruct.getLogger(__name__)


class KafkaOpenTelemetryTracing:
    """Small optional tracing adapter with fail-open behavior."""

    def __init__(
        self,
        *,
        enabled: bool = False,
        tracer: Any | None = None,
        propagator: Any | None = None,
        producer_span_kind: Any | None = None,
        consumer_span_kind: Any | None = None,
        client_span_kind: Any | None = None,
    ) -> None:
        self.enabled = enabled
        self._tracer = tracer
        self._propagator = propagator
        self._producer_span_kind = producer_span_kind
        self._consumer_span_kind = consumer_span_kind
        self._client_span_kind = client_span_kind
        if self.enabled:
            self._load_defaults()

    @classmethod
    def from_config(
        cls,
        tracing: bool | KafkaOpenTelemetryTracing,
    ) -> KafkaOpenTelemetryTracing:
        if isinstance(tracing, KafkaOpenTelemetryTracing):
            return tracing
        return cls(enabled=bool(tracing))

    def inject_headers(
        self,
        headers: list[tuple[str, bytes]] | None,
    ) -> list[tuple[str, bytes]] | None:
        if not self.enabled or self._propagator is None:
            return headers
        original = list(headers or [])
        carrier = _headers_to_carrier(original)
        before = dict(carrier)
        try:
            self._propagator.inject(carrier)
        except Exception as exc:
            logger.warning("kafka_tracing_inject_failed", error=str(exc))
            return headers

        changed_names = {
            name for name, value in carrier.items() if name not in before or before[name] != value
        }
        if not changed_names:
            return headers
        merged = [(name, value) for name, value in original if name not in changed_names]
        merged.extend(
            (name, value.encode("utf-8"))
            for name, value in carrier.items()
            if name in changed_names
        )
        return merged

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        kind: str,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        if not self.enabled or self._tracer is None:
            yield None
            return
        context = None
        if headers and self._propagator is not None:
            try:
                context = self._propagator.extract(_headers_to_carrier(headers))
            except Exception as exc:
                logger.warning("kafka_tracing_extract_failed", error=str(exc))
        try:
            span_cm = self._tracer.start_as_current_span(
                name,
                kind=self._span_kind(kind),
                context=context,
                attributes=attributes or {},
            )
        except Exception as exc:
            logger.warning("kafka_tracing_start_span_failed", span=name, error=str(exc))
            yield None
            return
        with span_cm as span:
            yield span

    def _span_kind(self, kind: str) -> Any | None:
        if kind == "producer":
            return self._producer_span_kind
        if kind == "consumer":
            return self._consumer_span_kind
        if kind == "client":
            return self._client_span_kind
        return None

    def _load_defaults(self) -> None:
        if self._tracer is not None and self._propagator is not None:
            return
        try:
            from opentelemetry import trace
            from opentelemetry.propagate import get_global_textmap
            from opentelemetry.trace import SpanKind
        except Exception as exc:
            logger.warning("kafka_tracing_unavailable", error=str(exc))
            self.enabled = False
            return
        if self._tracer is None:
            self._tracer = trace.get_tracer("agora_plugins.kafka")
        if self._propagator is None:
            self._propagator = get_global_textmap()
        self._producer_span_kind = self._producer_span_kind or SpanKind.PRODUCER
        self._consumer_span_kind = self._consumer_span_kind or SpanKind.CONSUMER
        self._client_span_kind = self._client_span_kind or SpanKind.CLIENT


def _headers_to_carrier(
    headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...],
) -> dict[str, str]:
    carrier: dict[str, str] = {}
    for name, value in headers:
        carrier[str(name)] = bytes(value).decode("utf-8", errors="replace")
    return carrier


__all__ = ["KafkaOpenTelemetryTracing"]
