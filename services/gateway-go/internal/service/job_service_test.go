package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"microservices-lab/gateway-go/internal/domain"
)

const contractsDir = "../../../../contracts"

// fakeStore guarda o que o serviço tentou persistir, sem banco.
type fakeStore struct {
	enqueued []domain.Envelope
}

func (s *fakeStore) Enqueue(_ context.Context, envelope domain.Envelope, _ domain.Target, _ time.Time) error {
	s.enqueued = append(s.enqueued, envelope)
	return nil
}
func (s *fakeStore) Find(context.Context, string) (*domain.JobView, error) { return nil, nil }
func (s *fakeStore) ListRecent(context.Context, int) ([]domain.JobView, error) {
	return nil, nil
}
func (s *fakeStore) ListOutbox(context.Context, domain.OutboxState, int) ([]domain.OutboxEntry, error) {
	return nil, nil
}
func (s *fakeStore) Ping(context.Context) error { return nil }

func newService(t *testing.T) (*JobService, *fakeStore) {
	t.Helper()
	validator, err := domain.NewPayloadValidator(contractsDir + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	fixed := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	return NewJobService(store, validator, func() time.Time { return fixed }), store
}

// TestSubmitBuildsEnvelopeThatMatchesContract garante que o envelope gravado na outbox
// obedece contracts/envelope.schema.json, que é o que os workers validam ao consumir.
func TestSubmitBuildsEnvelopeThatMatchesContract(t *testing.T) {
	svc, store := newService(t)

	jobID, err := svc.Submit(context.Background(), domain.JobIOSleep, domain.TargetGo, json.RawMessage(`{"ms": 5}`))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(store.enqueued) != 1 || store.enqueued[0].JobID != jobID {
		t.Fatalf("envelope não foi persistido: %+v", store.enqueued)
	}

	schema, err := jsonschema.NewCompiler().Compile(contractsDir + "/envelope.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(store.enqueued[0])
	instance, err := jsonschema.UnmarshalJSON(bytesReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(instance); err != nil {
		t.Errorf("envelope fora do contrato: %v\n%s", err, raw)
	}
}

// TestSubmitInvalidPayloadWritesNothing garante que pedido inválido não deixa rastro no banco.
func TestSubmitInvalidPayloadWritesNothing(t *testing.T) {
	svc, store := newService(t)

	_, err := svc.Submit(context.Background(), domain.JobIOSleep, domain.TargetAll, json.RawMessage(`{"ms": -5}`))

	if err == nil || len(store.enqueued) != 0 {
		t.Errorf("err=%v enqueued=%d; esperava erro e nada persistido", err, len(store.enqueued))
	}
}
