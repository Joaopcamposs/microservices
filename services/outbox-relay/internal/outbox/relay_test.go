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
	return NewRelay(store, publisher, m, 100, time.Millisecond, log), m
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
