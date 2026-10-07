package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/worker-go/internal/domain"
)

// Integração: exige o Postgres do compose; sem ele o teste é pulado.
func TestSaveIsIdempotent(t *testing.T) {
	url := os.Getenv("WORKER_DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:lab@localhost:5432/lab"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Skipf("postgres indisponível: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres indisponível: %v", err)
	}

	// job_results tem FK para jobs: cria o job pai e remove tudo no fim.
	id := uuid.NewString()
	const insertJob = `INSERT INTO jobs (job_id, type, target, origin)
		VALUES ($1::uuid, 'io.sleep', 'go', 'gateway-go')`
	if _, err := pool.Exec(ctx, insertJob, id); err != nil {
		t.Skipf("schema indisponível: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM job_results WHERE job_id = $1::uuid`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM jobs WHERE job_id = $1::uuid`, id)
	})

	repo := NewResultRepository(pool)
	now := time.Now()
	result := domain.JobResult{
		JobID: id, Worker: domain.WorkerName, Status: domain.StatusSucceeded,
		StartedAt: now, FinishedAt: now, Result: json.RawMessage(`{"slept_ms":0}`),
	}
	first, err := repo.Save(ctx, result)
	if err != nil || !first {
		t.Fatalf("primeira gravação: saved=%v err=%v", first, err)
	}
	second, err := repo.Save(ctx, result)
	if err != nil || second {
		t.Fatalf("duplicata: saved=%v err=%v", second, err)
	}
}
