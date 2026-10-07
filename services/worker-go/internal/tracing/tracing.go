// Package tracing configura o OpenTelemetry do worker: provider de traces e continuação do trace
// que veio no envelope.
package tracing

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// otlpEndpointEnv é a variável padrão do OpenTelemetry; o exporter OTLP/HTTP também a lê sozinho
// (inclusive o caminho /v1/traces). Sem ela os spans existem mas não são exportados: testes e
// execução local funcionam sem collector.
const otlpEndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// NewProvider cria o provider e liga o exporter OTLP só se o endpoint estiver configurado. Quem
// chama é dono do provider e deve chamar Shutdown para descarregar o lote pendente.
func NewProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
	}
	if os.Getenv(otlpEndpointEnv) != "" {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("criar exporter OTLP: %w", err)
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	return sdktrace.NewTracerProvider(options...), nil
}

// Continue devolve um contexto cujo span pai é o do traceparent do envelope, para o span do
// worker virar filho do span do gateway: é o que liga os dois serviços (e as duas linguagens) no
// mesmo trace. Traceparent inválido devolve ctx intacto e o span vira raiz de um trace novo.
func Continue(ctx context.Context, traceparent string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
