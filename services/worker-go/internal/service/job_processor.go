// Package service implementa o caso de uso do worker: validar, executar o handler e gravar o
// resultado. Depende de interfaces, não do Postgres nem do RabbitMQ.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"microservices-lab/worker-go/internal/domain"
	"microservices-lab/worker-go/internal/metrics"
)

// ResultStore é a porta de persistência do resultado. Fica aqui (lado do consumidor) para o
// processor depender de um contrato; nos testes entra um fake em memória.
type ResultStore interface {
	// Save grava o resultado; devolve false se (job_id, worker) já existia (entrega duplicada).
	Save(ctx context.Context, result domain.JobResult) (bool, error)
}

// JobProcessor valida, executa o handler e grava o resultado.
//
// Falha do **handler** é resultado (`failed` gravado e mensagem confirmada): reexecutar o mesmo
// erro não ajuda. Falha de **infraestrutura** (banco fora) volta como erro para o consumidor
// decidir o ack, porque nada foi gravado.
type JobProcessor struct {
	validator *domain.ContractValidator
	handlers  map[domain.JobType]domain.Handler
	store     ResultStore
	metrics   *metrics.Metrics
	log       *slog.Logger
	now       func() time.Time
}

// NewJobProcessor injeta as dependências. O relógio é parâmetro para os testes fixarem o tempo.
func NewJobProcessor(
	validator *domain.ContractValidator,
	handlers map[domain.JobType]domain.Handler,
	store ResultStore,
	m *metrics.Metrics,
	log *slog.Logger,
	now func() time.Time,
) *JobProcessor {
	return &JobProcessor{validator: validator, handlers: handlers, store: store, metrics: m, log: log, now: now}
}

// Process processa uma mensagem. Devolve erro embrulhando domain.ErrInvalidMessage se ela
// nunca poderá passar, ou o erro de infraestrutura se o resultado não pôde ser gravado.
func (p *JobProcessor) Process(ctx context.Context, body []byte) error {
	job, err := p.validator.Parse(body)
	if err != nil {
		return err
	}
	handler, ok := p.handlers[job.Type]
	if !ok {
		return fmt.Errorf("%w: sem handler para o tipo %s", domain.ErrInvalidMessage, job.Type)
	}
	result := p.run(ctx, job, handler)
	saved, err := p.store.Save(ctx, result)
	if err != nil {
		return fmt.Errorf("gravar resultado: %w", err)
	}
	status := string(result.Status)
	if !saved {
		status = "duplicate"
	}
	p.metrics.Processed.WithLabelValues(status).Inc()
	p.log.Info("job processado", "job_id", job.JobID, "status", status, "traceparent", job.Traceparent)
	return nil
}

// run executa o handler e converte sucesso ou erro em JobResult.
func (p *JobProcessor) run(ctx context.Context, job domain.Job, handler domain.Handler) domain.JobResult {
	startedAt := p.now()
	output, err := handler(ctx, job.Payload)
	finishedAt := p.now()
	p.metrics.Duration.Observe(finishedAt.Sub(startedAt).Seconds())

	result := domain.JobResult{
		JobID: job.JobID, Worker: domain.WorkerName, StartedAt: startedAt, FinishedAt: finishedAt,
		Status: domain.StatusSucceeded, Result: output,
	}
	if err != nil {
		message := err.Error()
		result.Status, result.Result, result.Error = domain.StatusFailed, nil, &message
	}
	return result
}
