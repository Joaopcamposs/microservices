// Package tracing configura o OpenTelemetry do gateway: provider de traces e a leitura do
// traceparent do span ativo, que viaja dentro do envelope.
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
// (inclusive o caminho /v1/traces). Sem ela os spans existem (o traceparent do envelope continua
// válido), mas não são exportados: testes e `make run-go` funcionam sem collector.
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

// Traceparent serializa o span ativo de ctx no formato W3C (`00-<trace>-<span>-<flags>`).
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}
