package api

import (
	"testing"

	// Registra o spec do Swagger (gerado por `make swagger`) para o teste da UI.
	_ "microservices-lab/gateway-go/docs"
)

// TestMain existe só para o import acima: o handler do Swagger lê o spec registrado por ele.
func TestMain(m *testing.M) { m.Run() }
