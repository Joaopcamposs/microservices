package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"microservices-lab/outbox-relay/internal/metrics"
)

// fakeStore reserva sempre as mesmas mensagens e repassa o resultado do publish como o Store real.
type fakeStore struct {
	messages []Message
	calls    atomic.Int32
	marked   []int64
}

func (s *fakeStore) WithBatch(ctx context.Context, _ int, publish PublishFunc) (int, error) {
	s.calls.Add(1)
	if len(s.messages) == 0 {
		return 0, nil
	}
	ids, err := publish(ctx, s.messages)
	s.marked = append(s.marked, ids...)
	return len(ids), err
}

// fakePublisher confirma só os IDs configurados e devolve o erro configurado.
type fakePublisher struct {
	confirm []int64
	err     error
}

func (p *fakePublisher) Publish(context.Context, []Message) ([]int64, error) {
	return p.confirm, p.err
}

func newTestRelay(store Store, publisher Publisher) (*Relay, *metrics.Metrics) {
	m := metrics.New(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRelay(store, publisher, m, noop.NewTracerProvider().Tracer("test"), 100, time.Millisecond, log), m
}

// TestTickMeasuresLagOnlyForConfirmedMessages garante que a métrica de lag e o contador de
// publicadas ignoram mensagens que o broker não confirmou: contar as não confirmadas
// esconderia justamente a falha que o benchmark quer enxergar.
func TestTickMeasuresLagOnlyForConfirmedMessages(t *testing.T) {
	created := time.Now().Add(-2 * time.Second)
	store := &fakeStore{messages: []Message{{ID: 1, CreatedAt: created}, {ID: 2, CreatedAt: created}}}
	relay, m := newTestRelay(store, &fakePublisher{confirm: []int64{1}, err: errors.New("nack no 2")})

	published, err := relay.Tick(context.Background())

	if err == nil {
		t.Fatal("esperava o erro do publisher propagado")
	}
	if published != 1 || len(store.marked) != 1 || store.marked[0] != 1 {
		t.Errorf("só o ID 1 devia ser marcado: published=%d marked=%v", published, store.marked)
	}
	if got := testutil.ToFloat64(m.Published); got != 1 {
		t.Errorf("outbox_published_total = %v, esperava 1", got)
	}
}

// TestRunCountsErrorsKeepsRunningAndStopsOnCancel garante que uma falha não derruba o laço
// (o relay precisa sobreviver a um broker fora do ar) e que cancelar o contexto o encerra.
func TestRunCountsErrorsKeepsRunningAndStopsOnCancel(t *testing.T) {
	store := &fakeStore{messages: []Message{{ID: 1, CreatedAt: time.Now()}}}
	relay, m := newTestRelay(store, &fakePublisher{err: errors.New("broker fora")})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { relay.Run(ctx); close(done) }()

	deadline := time.After(3 * time.Second)
	for testutil.ToFloat64(m.PublishErrors) < 1 {
		select {
		case <-deadline:
			t.Fatal("o erro do lote não foi contado")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run não encerrou após cancelar o contexto")
	}
}

// TestPublishSpanContinuesTheTraceFromTheEnvelope garante que o span do relay é filho do span do
// gateway: sem isso o trace quebra entre o enfileiramento e a publicação.
func TestPublishSpanContinuesTheTraceFromTheEnvelope(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	envelope := []byte(`{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}`)
	store := &fakeStore{messages: []Message{{ID: 1, Envelope: envelope, CreatedAt: time.Now()}}}
	relay, _ := newTestRelay(store, &fakePublisher{confirm: []int64{1}})
	relay.tracer = provider.Tracer("test")

	if _, err := relay.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, quer 1", len(spans))
	}
	if got := spans[0].SpanContext().TraceID().String(); got != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace id = %s", got)
	}
	if got := spans[0].Parent().SpanID().String(); got != "b7ad6b7169203331" {
		t.Errorf("parent = %s", got)
	}
}
