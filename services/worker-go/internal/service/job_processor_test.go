package service

import (
	"context"
	"encoding/json"
	"errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"microservices-lab/worker-go/internal/domain"
	"microservices-lab/worker-go/internal/metrics"
)

const jobID = "8b1f6a5e-3c2d-4e1a-9f7b-0a1b2c3d4e5f"

// fakeStore guarda resultados em memória e imita a idempotência do banco.
type fakeStore struct {
	results map[string]domain.JobResult
	err     error
}

func (s *fakeStore) Save(_ context.Context, r domain.JobResult) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if _, ok := s.results[r.JobID]; ok {
		return false, nil
	}
	s.results[r.JobID] = r
	return true, nil
}

func newStore() *fakeStore { return &fakeStore{results: map[string]domain.JobResult{}} }

// envelope monta uma mensagem válida do contrato com o payload dado.
func envelope(jobType, payload string) []byte {
	return []byte(`{"job_id":"` + jobID + `","type":"` + jobType + `","origin":"gateway-go",` +
		`"payload":` + payload + `,"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",` +
		`"attempt":1,"created_at":"2026-01-01T00:00:00Z"}`)
}

// newProcessor monta o processor com o contrato real e o handler de io.sleep dado.
func newProcessor(t *testing.T, handler domain.Handler, store *fakeStore) *JobProcessor {
	t.Helper()
	return newTracedProcessor(t, handler, store, sdktrace.NewTracerProvider().Tracer("test"))
}

// newTracedProcessor é o newProcessor com o tracer escolhido pelo teste.
func newTracedProcessor(t *testing.T, handler domain.Handler, store *fakeStore, tracer trace.Tracer) *JobProcessor {
	t.Helper()
	validator, err := domain.NewContractValidator(filepath.Join("..", "..", "..", "..", "contracts"))
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handlers := map[domain.JobType]domain.Handler{domain.JobIOSleep: handler}
	return NewJobProcessor(validator, handlers, store, m, tracer, log, time.Now)
}

func okHandler(context.Context, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"ok":true}`), nil
}

func TestProcessSavesSucceeded(t *testing.T) {
	store := newStore()
	p := newProcessor(t, okHandler, store)
	if err := p.Process(context.Background(), envelope("io.sleep", `{"ms":0}`)); err != nil {
		t.Fatal(err)
	}
	got := store.results[jobID]
	if got.Status != domain.StatusSucceeded || got.Worker != "go" {
		t.Fatalf("resultado inesperado: %+v", got)
	}
}

// Falha do handler é resultado `failed` gravado, e não erro (a mensagem é confirmada).
func TestProcessHandlerFailureIsSavedAsFailed(t *testing.T) {
	store := newStore()
	failing := func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	}
	p := newProcessor(t, failing, store)
	if err := p.Process(context.Background(), envelope("io.sleep", `{"ms":0}`)); err != nil {
		t.Fatal(err)
	}
	got := store.results[jobID]
	if got.Status != domain.StatusFailed || got.Error == nil || *got.Error != "boom" {
		t.Fatalf("resultado inesperado: %+v", got)
	}
}

// Entrega duplicada não regrava nem falha.
func TestProcessDuplicateIsIgnored(t *testing.T) {
	store := newStore()
	p := newProcessor(t, okHandler, store)
	body := envelope("io.sleep", `{"ms":0}`)
	for range 2 {
		if err := p.Process(context.Background(), body); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.results) != 1 {
		t.Fatalf("esperava 1 resultado, há %d", len(store.results))
	}
}

// Mensagens que violam o contrato viram ErrInvalidMessage (destino: DLQ).
func TestProcessInvalidMessages(t *testing.T) {
	store := newStore()
	p := newProcessor(t, okHandler, store)
	bodies := map[string][]byte{
		"json quebrado":     []byte(`{nope`),
		"payload inválido":  envelope("io.sleep", `{"ms":-5}`),
		"ms acima do máx":   envelope("io.sleep", `{"ms":60001}`),
		"tipo desconhecido": envelope("io.nope", `{}`),
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			if err := p.Process(context.Background(), body); !errors.Is(err, domain.ErrInvalidMessage) {
				t.Fatalf("esperava ErrInvalidMessage, veio %v", err)
			}
		})
	}
	if len(store.results) != 0 {
		t.Fatal("nada deveria ter sido gravado")
	}
}

// Falha de infra volta como erro (não inválido) para o consumidor decidir o ack.
func TestProcessStoreFailureIsInfraError(t *testing.T) {
	store := &fakeStore{err: errors.New("banco fora")}
	p := newProcessor(t, okHandler, store)
	err := p.Process(context.Background(), envelope("io.sleep", `{"ms":0}`))
	if err == nil || errors.Is(err, domain.ErrInvalidMessage) {
		t.Fatalf("esperava erro de infra, veio %v", err)
	}
}

// O span do worker é filho do span do gateway, identificado pelo traceparent do envelope.
func TestProcessContinuesTheTraceFromTheEnvelope(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	p := newTracedProcessor(t, okHandler, newStore(), provider.Tracer("test"))
	if err := p.Process(context.Background(), envelope("io.sleep", `{"ms":0}`)); err != nil {
		t.Fatal(err)
	}
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		byName[span.Name()] = span
	}
	root := byName["process io.sleep"]
	if root == nil || byName["run handler"] == nil || byName["save result"] == nil {
		t.Fatalf("spans = %v", byName)
	}
	if got := root.SpanContext().TraceID().String(); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace id = %s", got)
	}
	if got := root.Parent().SpanID().String(); got != "b7ad6b7169203331" {
		t.Errorf("parent span id = %s", got)
	}
	for _, child := range []string{"run handler", "save result"} {
		if byName[child].Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("%s não é filho de process", child)
		}
	}
}
