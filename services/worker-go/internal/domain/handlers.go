package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Handler executa um tipo de job e devolve o JSON do resultado. O payload já foi validado
// pelo schema antes de chegar aqui.
type Handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

// NewHandlers devolve o registro tipo -> handler. Tipo ausente aqui vai para a DLQ (não há
// como processá-lo). Handler CPU-bound novo roda na própria goroutine do pool: o scheduler do
// Go distribui entre os núcleos, sem o cuidado de `to_thread` que o worker Python exige.
func NewHandlers() map[JobType]Handler {
	return map[JobType]Handler{
		JobIOSleep:           handleIOSleep,
		JobCPUPBKDF2:         handleCPUPBKDF2,
		JobIOFetchURLs:       handleIOFetchURLs,
		JobDataJSONTransform: handleDataJSONTransform,
	}
}

// handleIOSleep dorme `ms` milissegundos sem ocupar uma thread do SO (a goroutine é
// estacionada pelo scheduler). Mede o overhead puro do framework: o trabalho é só espera, então
// a diferença de vazão entre as stacks vem do consumo da fila e não do handler. Respeita o
// cancelamento do contexto para não segurar o encerramento.
func handleIOSleep(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Ms int `json:"ms"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("payload io.sleep: %w", err)
	}
	timer := time.NewTimer(time.Duration(input.Ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return json.RawMessage(fmt.Sprintf(`{"slept_ms": %d}`, input.Ms)), nil
}
