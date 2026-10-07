// Package api é a borda HTTP do gateway (Gin). Cada handler só traduz HTTP para chamadas do
// JobService. As anotações `@...` geram o Swagger em /swagger (ver `make swagger`).
package api

import (
	"encoding/json"
	"time"

	"microservices-lab/gateway-go/internal/domain"
)

// JobPayload documenta o corpo de POST /jobs para o Swagger. O corpo real é validado contra
// contracts/jobs/<type>.schema.json; este tipo mostra o único schema existente (io.sleep) para
// o "Try it out" já vir preenchido.
type JobPayload struct {
	// Ms é a duração do sleep em milissegundos (0 a 60000) quando type=io.sleep.
	Ms int `json:"ms" example:"100"`
}

// JobAccepted é a resposta 202 do POST /jobs: só o identificador para consultar depois.
type JobAccepted struct {
	JobID string `json:"job_id" example:"7c9e6679-7425-40de-944b-e07fc1f90ae7"`
}

// ErrorBody é o formato dos erros (422, 404, 503), igual ao `detail` do gateway-py.
type ErrorBody struct {
	Detail string `json:"detail" example:"job não encontrado"`
}

// StatusBody é a resposta do /healthz.
type StatusBody struct {
	Status string `json:"status" example:"ok"`
}

// WorkerResultOut é o resultado de um worker dentro de JobOut.
type WorkerResultOut struct {
	Worker     string          `json:"worker" example:"asyncio"`
	Status     string          `json:"status" enums:"succeeded,failed"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
	Result     json.RawMessage `json:"result" swaggertype:"object"`
	Error      *string         `json:"error"`
}

// JobOut é o job com status agregado e resultados (GET /jobs e GET /jobs/{id}).
type JobOut struct {
	JobID     string            `json:"job_id" example:"7c9e6679-7425-40de-944b-e07fc1f90ae7"`
	Type      string            `json:"type" example:"io.sleep"`
	Target    string            `json:"target" enums:"all,celery,taskiq,asyncio,go"`
	Origin    string            `json:"origin" enums:"gateway-py,gateway-go"`
	CreatedAt time.Time         `json:"created_at"`
	Status    string            `json:"status" enums:"pending,running,completed"`
	Results   []WorkerResultOut `json:"results"`
}

// OutboxEntryOut é a linha da outbox exposta em GET /outbox.
type OutboxEntryOut struct {
	ID          int64      `json:"id"`
	JobID       string     `json:"job_id"`
	RoutingKey  string     `json:"routing_key"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at"`
}

// toJobOut converte o modelo de domínio no modelo de resposta da API.
func toJobOut(view domain.JobView) JobOut {
	results := make([]WorkerResultOut, 0, len(view.Results))
	for _, result := range view.Results {
		results = append(results, WorkerResultOut{
			Worker: result.Worker, Status: result.Status,
			StartedAt: result.StartedAt, FinishedAt: result.FinishedAt,
			Result: nullIfEmpty(result.Result), Error: result.Error,
		})
	}
	return JobOut{
		JobID: view.Record.JobID, Type: string(view.Record.Type), Target: string(view.Record.Target),
		Origin: string(view.Record.Origin), CreatedAt: view.Record.CreatedAt,
		Status: string(view.Status()), Results: results,
	}
}

// nullIfEmpty garante JSON válido: json.RawMessage vazio não serializa, então vira null.
func nullIfEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

// toOutboxOut converte as linhas da outbox no modelo de resposta.
func toOutboxOut(entries []domain.OutboxEntry) []OutboxEntryOut {
	out := make([]OutboxEntryOut, 0, len(entries))
	for _, entry := range entries {
		out = append(out, OutboxEntryOut{
			ID: entry.ID, JobID: entry.JobID, RoutingKey: entry.RoutingKey,
			CreatedAt: entry.CreatedAt, PublishedAt: entry.PublishedAt,
		})
	}
	return out
}
