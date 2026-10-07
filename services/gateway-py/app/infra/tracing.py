"""OpenTelemetry do gateway: provider de traces e leitura do `traceparent` do span ativo."""

import os

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Variável padrão do OpenTelemetry: o exporter OTLP/HTTP também a lê sozinho (inclusive o caminho
# `/v1/traces`). Sem ela, os spans existem (o `traceparent` do envelope continua válido), mas
# não são exportados, o que mantém os testes e o `make run` sem collector.
OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"


def build_tracer_provider(service_name: str) -> TracerProvider:
    """Cria o provider e liga o exporter OTLP apenas se o endpoint estiver configurado.

    O provider é devolvido, e não registrado como global, para o processo dono (`Services`)
    controlar o ciclo de vida e chamar `shutdown()` (que descarrega o lote pendente).
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if os.environ.get(OTLP_ENDPOINT_ENV):
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    return provider


def current_traceparent() -> str:
    """Serializa o span ativo como `traceparent` W3C, para viajar dentro do envelope.

    Deve ser chamada dentro de um span; fora de um, o carrier volta vazio e a chamada falha.
    """
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    return carrier["traceparent"]
