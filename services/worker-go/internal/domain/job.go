// Package domain reúne as regras puras do worker: contrato, modelos e handlers dos jobs. Não
// importa banco nem broker.
package domain

import (
	"encoding/json"
	"time"
)

// WorkerName é o valor gravado em job_results.worker; a constraint do banco só aceita os
// quatro workers.
const WorkerName = "go"

// JobType é o tipo de job do contrato (contracts/envelope.schema.json).
type JobType string

// Tipos de job do contrato.
const (
	JobIOSleep           JobType = "io.sleep"
	JobIOFetchURLs       JobType = "io.fetch_urls"
	JobCPUPBKDF2         JobType = "cpu.pbkdf2"
	JobDataJSONTransform JobType = "data.json_transform"
	JobPipelineFanout    JobType = "pipeline.fanout"
)

// AllJobTypes lista os tipos do contrato; o validador usa para carregar os schemas existentes.
var AllJobTypes = []JobType{
	JobIOSleep, JobIOFetchURLs, JobCPUPBKDF2, JobDataJSONTransform, JobPipelineFanout,
}

// ResultStatus é o resultado final do worker para um job (job_results.status).
type ResultStatus string

// Resultados possíveis.
const (
	StatusSucceeded ResultStatus = "succeeded"
	StatusFailed    ResultStatus = "failed"
)

// Job é o envelope já validado, só com os campos que o worker usa. Traceparent e Attempt
// seguem junto para os logs; a propagação completa do trace entra na fase de observabilidade.
type Job struct {
	JobID       string          `json:"job_id"`
	Type        JobType         `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Traceparent string          `json:"traceparent"`
	Attempt     int             `json:"attempt"`
}

// JobResult é a linha de job_results: uma por (job_id, worker), gravada uma única vez.
type JobResult struct {
	JobID      string
	Worker     string
	Status     ResultStatus
	StartedAt  time.Time
	FinishedAt time.Time
	// Result é o JSON produzido pelo handler, ou nil quando falhou.
	Result json.RawMessage
	// Error descreve a falha do handler; nil quando teve sucesso.
	Error *string
}
