// Package service implementa os casos de uso do gateway: validar o pedido, montar o envelope
// e entregá-lo à outbox. Depende de interfaces, não do Postgres.
package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"microservices-lab/gateway-go/internal/domain"
	"microservices-lab/gateway-go/internal/tracing"
)

// JobStore é a porta de persistência exigida pelo serviço. Fica aqui (lado do consumidor)
// para o serviço depender de um contrato; nos testes entra um fake, em produção o Postgres.
type JobStore interface {
	// Enqueue grava o job e a linha da outbox atomicamente.
	Enqueue(ctx context.Context, envelope domain.Envelope, target domain.Target, createdAt time.Time) error
	// Find busca um job com resultados; devolve (nil, nil) se não existe.
	Find(ctx context.Context, jobID string) (*domain.JobView, error)
	// ListRecent lista jobs recentes com resultados.
	ListRecent(ctx context.Context, limit int) ([]domain.JobView, error)
	// ListOutbox lista linhas da outbox por estado.
	ListOutbox(ctx context.Context, state domain.OutboxState, limit int) ([]domain.OutboxEntry, error)
	// Ping falha se o armazenamento está indisponível.
	Ping(ctx context.Context) error
}

// JobService é o caso de uso: validar o pedido, montar o envelope e entregá-lo à outbox.
type JobService struct {
	store     JobStore
	validator *domain.PayloadValidator
	tracer    trace.Tracer
	now       func() time.Time
}

// NewJobService injeta as dependências. O relógio é parâmetro para os testes fixarem o tempo.
func NewJobService(store JobStore, validator *domain.PayloadValidator, tracer trace.Tracer, now func() time.Time) *JobService {
	return &JobService{store: store, validator: validator, tracer: tracer, now: now}
}

// Submit valida o payload, monta o envelope e o grava na outbox; devolve o job_id.
//
// A validação vem antes de qualquer escrita: pedido inválido não deixa rastro no banco. O
// job_id nasce aqui (UUID v4) porque é a chave de idempotência dos workers.
func (s *JobService) Submit(ctx context.Context, jobType domain.JobType, target domain.Target, payload json.RawMessage) (string, error) {
	if err := s.validator.Validate(jobType, payload); err != nil {
		return "", err
	}
	// Span PRODUCER: o traceparent gravado no envelope é o dele, então relay e workers continuam
	// o trace a partir daqui.
	ctx, span := s.tracer.Start(ctx, "enqueue job", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("job.type", string(jobType)), attribute.String("job.target", string(target))))
	defer span.End()
	createdAt := s.now()
	envelope := domain.Envelope{
		JobID:       uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		CreatedAt:   domain.FormatCreatedAt(createdAt),
		Traceparent: tracing.Traceparent(ctx),
		Attempt:     0,
		Origin:      domain.OriginGatewayGo,
	}
	span.SetAttributes(attribute.String("job.id", envelope.JobID))
	if err := s.store.Enqueue(ctx, envelope, target, createdAt); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	return envelope.JobID, nil
}

// Get consulta um job com os resultados por worker; (nil, nil) se não existe.
func (s *JobService) Get(ctx context.Context, jobID string) (*domain.JobView, error) {
	return s.store.Find(ctx, jobID)
}

// ListRecent lista os jobs mais recentes.
func (s *JobService) ListRecent(ctx context.Context, limit int) ([]domain.JobView, error) {
	return s.store.ListRecent(ctx, limit)
}

// ListOutbox lista a outbox para depurar o fluxo gateway -> relay.
func (s *JobService) ListOutbox(ctx context.Context, state domain.OutboxState, limit int) ([]domain.OutboxEntry, error) {
	return s.store.ListOutbox(ctx, state, limit)
}

// CheckHealth verifica a conexão com o armazenamento.
func (s *JobService) CheckHealth(ctx context.Context) error {
	return s.store.Ping(ctx)
}
