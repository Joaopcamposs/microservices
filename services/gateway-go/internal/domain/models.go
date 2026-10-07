package domain

import (
	"encoding/json"
	"time"
)

// Envelope é a mensagem neutra definida em contracts/envelope.schema.json: o contrato único
// entre gateways e workers. As tags fixam os nomes do JSON e a ordem dos campos.
type Envelope struct {
	JobID       string          `json:"job_id"`
	Type        JobType         `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   string          `json:"created_at"`
	Traceparent string          `json:"traceparent"`
	Attempt     int             `json:"attempt"`
	Origin      Origin          `json:"origin"`
}

// createdAtLayout é RFC 3339 em UTC com microssegundos e sufixo Z, como o schema (date-time)
// espera e como o gateway-py serializa.
const createdAtLayout = "2006-01-02T15:04:05.000000Z"

// FormatCreatedAt serializa o instante no formato do envelope.
func FormatCreatedAt(t time.Time) string {
	return t.UTC().Format(createdAtLayout)
}

// JobRecord é a linha da tabela jobs: o pedido original, sem os resultados.
type JobRecord struct {
	JobID     string
	Type      JobType
	Target    Target
	Origin    Origin
	CreatedAt time.Time
}

// WorkerResult é a linha de job_results: o que um worker específico produziu para o job.
type WorkerResult struct {
	Worker     string
	Status     string
	StartedAt  time.Time
	FinishedAt time.Time
	// Result é o JSON do resultado, ou nil quando o worker falhou.
	Result json.RawMessage
	Error  *string
}

// OutboxEntry é a linha da outbox sem o envelope (que pode ser grande); usada para inspeção.
type OutboxEntry struct {
	ID          int64
	JobID       string
	RoutingKey  string
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// JobView junta o job aos resultados dos workers: o que a consulta da API devolve.
type JobView struct {
	Record  JobRecord
	Results []WorkerResult
}

// ExpectedWorkers diz quantos resultados o job espera: 4 no fanout, 1 com target específico.
func (v JobView) ExpectedWorkers() int {
	if v.Record.Target == TargetAll {
		return workersPerFanout
	}
	return 1
}

// Status deriva o estado dos resultados já gravados; não há coluna de status no banco.
// Calcular em vez de persistir evita um segundo write por worker e o risco de divergir de
// job_results, que é a fonte da verdade.
func (v JobView) Status() JobStatus {
	switch {
	case len(v.Results) == 0:
		return StatusPending
	case len(v.Results) >= v.ExpectedWorkers():
		return StatusCompleted
	default:
		return StatusRunning
	}
}
