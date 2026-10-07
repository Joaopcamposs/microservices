// Package service implementa o caso de uso do worker: validar, executar o handler e gravar o
// resultado. Depende de interfaces, não do Postgres nem do RabbitMQ.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"microservices-lab/worker-go/internal/domain"
	"microservices-lab/worker-go/internal/metrics"
	"microservices-lab/worker-go/internal/tracing"
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
	tracer    trace.Tracer
	log       *slog.Logger
	now       func() time.Time
}

// NewJobProcessor injeta as dependências. O relógio é parâmetro para os testes fixarem o tempo.
func NewJobProcessor(
	validator *domain.ContractValidator,
	handlers map[domain.JobType]domain.Handler,
	store ResultStore,
	m *metrics.Metrics,
	tracer trace.Tracer,
	log *slog.Logger,
	now func() time.Time,
) *JobProcessor {
	return &JobProcessor{validator: validator, handlers: handlers, store: store, metrics: m, tracer: tracer, log: log, now: now}
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
	// Span CONSUMER filho do span do gateway: o traceparent do envelope liga os dois serviços.
	ctx, span := p.tracer.Start(tracing.Continue(ctx, job.Traceparent), "process "+string(job.Type),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("job.id", job.JobID), attribute.String("job.type", string(job.Type)),
			attribute.String("worker", domain.WorkerName)))
	defer span.End()
	result := p.run(ctx, job, handler)
	saved, err := p.save(ctx, result)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("gravar resultado: %w", err)
	}
	status := string(result.Status)
	if !saved {
		status = "duplicate"
	}
	span.SetAttributes(attribute.String("job.status", status))
	if result.Status == domain.StatusFailed {
		span.SetStatus(codes.Error, *result.Error)
	}
	p.metrics.Processed.WithLabelValues(status).Inc()
	p.log.Info("job processado", "job_id", job.JobID, "status", status, "traceparent", job.Traceparent)
	return nil
}

// save grava o resultado dentro de um span próprio, para o trace mostrar o tempo no banco.
func (p *JobProcessor) save(ctx context.Context, result domain.JobResult) (bool, error) {
	ctx, span := p.tracer.Start(ctx, "save result")
	defer span.End()
	return p.store.Save(ctx, result)
}

// run executa o handler e converte sucesso ou erro em JobResult.
func (p *JobProcessor) run(ctx context.Context, job domain.Job, handler domain.Handler) domain.JobResult {
	ctx, span := p.tracer.Start(ctx, "run handler")
	defer span.End()
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
