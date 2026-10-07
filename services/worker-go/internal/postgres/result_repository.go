// Package postgres acessa o banco: grava o resultado do job em job_results.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/worker-go/internal/domain"
)

// insertResultQuery usa ON CONFLICT DO NOTHING para tornar a gravação idempotente: a entrega
// é at-least-once, então o mesmo job pode chegar duas vezes e o segundo resultado é descartado
// sem erro. $6::jsonb porque o resultado chega como texto JSON.
const insertResultQuery = `
INSERT INTO job_results (job_id, worker, status, started_at, finished_at, result, error)
VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7)
ON CONFLICT (job_id, worker) DO NOTHING`

// ResultRepository grava resultados usando um pool de conexões criado (e fechado) pelo main.
type ResultRepository struct {
	pool *pgxpool.Pool
}

// NewResultRepository recebe o pool pronto.
func NewResultRepository(pool *pgxpool.Pool) *ResultRepository {
	return &ResultRepository{pool: pool}
}

// Save grava o resultado; devolve false se já existia (entrega duplicada).
func (r *ResultRepository) Save(ctx context.Context, result domain.JobResult) (bool, error) {
	var payload *string
	if result.Result != nil {
		text := string(result.Result)
		payload = &text
	}
	tag, err := r.pool.Exec(ctx, insertResultQuery,
		result.JobID, result.Worker, string(result.Status), result.StartedAt, result.FinishedAt, payload, result.Error)
	if err != nil {
		return false, fmt.Errorf("inserir job_result: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
