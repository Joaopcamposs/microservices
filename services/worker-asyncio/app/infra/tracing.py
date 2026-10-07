"""OpenTelemetry do worker: provider de traces e continuação do trace vindo do envelope."""

import os

from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Variável padrão do OpenTelemetry: o exporter OTLP/HTTP também a lê sozinho (inclusive o caminho
# `/v1/traces`). Sem ela, os spans existem mas não são exportados (testes e `make run` sem
# collector).
OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"


def build_tracer_provider(service_name: str) -> TracerProvider:
    """Cria o provider e liga o exporter OTLP apenas se o endpoint estiver configurado.

    É devolvido, e não registrado como global, para o dono do processo controlar o ciclo de vida
    e chamar `shutdown()` (que descarrega o lote pendente de spans).
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if os.environ.get(OTLP_ENDPOINT_ENV):
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    return provider


def continue_trace(traceparent: str) -> Context:
    """Converte o `traceparent` do envelope em contexto, para o span do worker virar filho do
    span do gateway: é o que liga os dois serviços (e as duas linguagens) no mesmo trace.
    """
    return TraceContextTextMapPropagator().extract({"traceparent": traceparent})
