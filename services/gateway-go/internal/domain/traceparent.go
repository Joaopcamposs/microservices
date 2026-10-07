package domain

import (
	"crypto/rand"
	"encoding/hex"
)

// NewTraceparent gera um traceparent W3C amostrado com trace novo:
// `00-<trace_id 32 hex>-<span_id 16 hex>-01`. Provisório: com OpenTelemetry (fase 5) o valor
// passa a vir do span ativo.
func NewTraceparent() string {
	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	// crypto/rand.Read não falha em plataformas suportadas (Go 1.24+ garante isso).
	_, _ = rand.Read(traceID)
	_, _ = rand.Read(spanID)
	return "00-" + hex.EncodeToString(traceID) + "-" + hex.EncodeToString(spanID) + "-01"
}
