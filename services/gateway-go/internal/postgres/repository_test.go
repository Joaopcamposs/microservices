package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/gateway-go/internal/domain"
)

// Testes de integração: usam o Postgres do `make up` e são pulados se ele não responder.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("GATEWAY_DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:lab@localhost:5432/lab"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		t.Skipf("Postgres indisponível (rode `make up`): %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newEnvelope() domain.Envelope {
	return domain.Envelope{
		JobID: uuid.NewString(), Type: domain.JobIOSleep, Payload: []byte(`{"ms": 1}`),
		CreatedAt: domain.FormatCreatedAt(time.Now()), Traceparent: "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01",
		Origin: domain.OriginGatewayGo,
	}
}

func cleanup(t *testing.T, pool *pgxpool.Pool, jobID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM job_results WHERE job_id = $1::uuid`, jobID)
		_, _ = pool.Exec(ctx, `DELETE FROM outbox WHERE job_id = $1::uuid`, jobID)
		_, _ = pool.Exec(ctx, `DELETE FROM jobs WHERE job_id = $1::uuid`, jobID)
	})
}

// TestEnqueueWritesJobAndOutboxTogether garante o padrão outbox: uma chamada grava as duas
// linhas, com a routing key certa, e o job nasce pendente.
func TestEnqueueWritesJobAndOutboxTogether(t *testing.T) {
	pool, ctx := testPool(t), context.Background()
	repo := NewRepository(pool)
	envelope := newEnvelope()
	cleanup(t, pool, envelope.JobID)

	if err := repo.Enqueue(ctx, envelope, domain.TargetGo, time.Now()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var routingKey string
	var publishedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT routing_key, published_at FROM outbox WHERE job_id = $1::uuid`, envelope.JobID).
		Scan(&routingKey, &publishedAt); err != nil {
		t.Fatalf("linha da outbox ausente: %v", err)
	}
	if routingKey != "go" {
		t.Errorf("routing_key = %q", routingKey)
	}
	view, err := repo.Find(ctx, envelope.JobID)
	if err != nil || view == nil || view.Status() != domain.StatusPending {
		t.Fatalf("Find = %+v, %v; esperava job pendente", view, err)
	}
}

// TestEnqueueIsAtomic garante o "ou as duas, ou nenhuma": se a gravação falha, a outbox não
// fica com linha órfã. Forçamos a falha repetindo o job_id (PK de jobs).
func TestEnqueueIsAtomic(t *testing.T) {
	pool, ctx := testPool(t), context.Background()
	repo := NewRepository(pool)
	envelope := newEnvelope()
	cleanup(t, pool, envelope.JobID)
	if err := repo.Enqueue(ctx, envelope, domain.TargetAll, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := repo.Enqueue(ctx, envelope, domain.TargetAll, time.Now()); err == nil {
		t.Fatal("esperava erro ao repetir o job_id")
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE job_id = $1::uuid`, envelope.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("outbox tem %d linhas, esperava 1 (a segunda tentativa devia reverter)", count)
	}
}

// TestFindReflectsWorkerResults garante a derivação do status e a leitura do jsonb.
func TestFindReflectsWorkerResults(t *testing.T) {
	pool, ctx := testPool(t), context.Background()
	repo := NewRepository(pool)
	envelope := newEnvelope()
	cleanup(t, pool, envelope.JobID)
	if err := repo.Enqueue(ctx, envelope, domain.TargetAsyncio, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO job_results (job_id, worker, status, started_at, finished_at, result)
		 VALUES ($1::uuid, 'asyncio', 'succeeded', now(), now(), '{"slept_ms": 1}')`, envelope.JobID); err != nil {
		t.Fatal(err)
	}

	view, err := repo.Find(ctx, envelope.JobID)

	if err != nil || view == nil {
		t.Fatalf("Find = %v, %v", view, err)
	}
	if view.Status() != domain.StatusCompleted || string(view.Results[0].Result) != `{"slept_ms": 1}` {
		t.Errorf("status=%s result=%s", view.Status(), view.Results[0].Result)
	}
}
