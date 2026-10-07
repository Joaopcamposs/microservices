// Package postgres é a única parte do gateway que conhece as tabelas. Usa SQL explícito em
// vez de ORM: o schema é pequeno e compartilhado, e o SQL deixa visível o que cada operação
// faz no banco.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/gateway-go/internal/domain"
)

const (
	insertJobQuery = `INSERT INTO jobs (job_id, type, target, origin, created_at)
		VALUES ($1::uuid, $2, $3, $4, $5)`

	// $3::jsonb: o envelope chega como texto JSON pronto e é guardado como jsonb (que
	// normaliza espaços e ordem das chaves, mas preserva o conteúdo).
	insertOutboxQuery = `INSERT INTO outbox (job_id, routing_key, envelope)
		VALUES ($1::uuid, $2, $3::jsonb)`

	selectJobQuery = `SELECT job_id::text, type, target, origin, created_at
		FROM jobs WHERE job_id = $1::uuid`

	selectResultsQuery = `SELECT worker, status, started_at, finished_at, result::text, error
		FROM job_results WHERE job_id = $1::uuid ORDER BY worker`

	selectRecentJobsQuery = `SELECT job_id::text, type, target, origin, created_at
		FROM jobs ORDER BY created_at DESC LIMIT $1`

	// Uma query para os resultados de todos os jobs da listagem, evitando N+1 consultas.
	selectResultsOfManyQuery = `SELECT job_id::text, worker, status, started_at, finished_at, result::text, error
		FROM job_results WHERE job_id = ANY($1::uuid[]) ORDER BY worker`

	selectOutboxQuery = `SELECT id, job_id::text, routing_key, created_at, published_at FROM outbox
		WHERE ($1::text = 'all')
		   OR ($1::text = 'pending' AND published_at IS NULL)
		   OR ($1::text = 'published' AND published_at IS NOT NULL)
		ORDER BY id DESC LIMIT $2`
)

// Repository acessa o Postgres. O gateway nunca fala com o RabbitMQ: publicar é papel do relay.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository recebe o pool pronto; quem o cria (e fecha) é o main.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Enqueue grava jobs e outbox na mesma transação: ou as duas linhas existem, ou nenhuma.
//
// É o coração do padrão outbox: sem broker na transação, não há janela em que o job existe no
// banco mas a mensagem se perdeu (ou o contrário). routing_key vazia vai para o exchange
// fanout; preenchida, para o direct.
func (r *Repository) Enqueue(ctx context.Context, envelope domain.Envelope, target domain.Target, createdAt time.Time) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("serializar envelope: %w", err)
	}
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertJobQuery,
			envelope.JobID, string(envelope.Type), string(target), string(envelope.Origin), createdAt); err != nil {
			return fmt.Errorf("inserir job: %w", err)
		}
		if _, err := tx.Exec(ctx, insertOutboxQuery,
			envelope.JobID, target.RoutingKey(), string(body)); err != nil {
			return fmt.Errorf("inserir outbox: %w", err)
		}
		return nil
	})
}

// Find busca um job com os resultados dos workers; (nil, nil) se o job_id não existe.
func (r *Repository) Find(ctx context.Context, jobID string) (*domain.JobView, error) {
	record, err := scanJob(r.pool.QueryRow(ctx, selectJobQuery, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("buscar job: %w", err)
	}
	rows, err := r.pool.Query(ctx, selectResultsQuery, jobID)
	if err != nil {
		return nil, fmt.Errorf("buscar resultados: %w", err)
	}
	defer rows.Close()
	results := []domain.WorkerResult{}
	for rows.Next() {
		var result domain.WorkerResult
		var raw *string
		if err := rows.Scan(&result.Worker, &result.Status, &result.StartedAt, &result.FinishedAt, &raw, &result.Error); err != nil {
			return nil, fmt.Errorf("ler resultado: %w", err)
		}
		result.Result = rawJSON(raw)
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &domain.JobView{Record: record, Results: results}, nil
}

// ListRecent lista os jobs mais novos primeiro, cada um com seus resultados.
func (r *Repository) ListRecent(ctx context.Context, limit int) ([]domain.JobView, error) {
	rows, err := r.pool.Query(ctx, selectRecentJobsQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("listar jobs: %w", err)
	}
	defer rows.Close()
	views := []domain.JobView{}
	jobIDs := []string{}
	for rows.Next() {
		record, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("ler job: %w", err)
		}
		views = append(views, domain.JobView{Record: record, Results: []domain.WorkerResult{}})
		jobIDs = append(jobIDs, record.JobID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(views) == 0 {
		return views, nil
	}
	return r.attachResults(ctx, views, jobIDs)
}

// attachResults busca em uma query os resultados de todos os jobs e os distribui nas views.
func (r *Repository) attachResults(ctx context.Context, views []domain.JobView, jobIDs []string) ([]domain.JobView, error) {
	rows, err := r.pool.Query(ctx, selectResultsOfManyQuery, jobIDs)
	if err != nil {
		return nil, fmt.Errorf("listar resultados: %w", err)
	}
	defer rows.Close()
	index := make(map[string]int, len(views))
	for i, view := range views {
		index[view.Record.JobID] = i
	}
	for rows.Next() {
		var jobID string
		var result domain.WorkerResult
		var raw *string
		if err := rows.Scan(&jobID, &result.Worker, &result.Status, &result.StartedAt, &result.FinishedAt, &raw, &result.Error); err != nil {
			return nil, fmt.Errorf("ler resultado: %w", err)
		}
		result.Result = rawJSON(raw)
		i := index[jobID]
		views[i].Results = append(views[i].Results, result)
	}
	return views, rows.Err()
}

// ListOutbox lista linhas da outbox (mais novas primeiro) filtradas por estado. Serve para
// inspecionar se o relay está esvaziando a fila; não devolve o envelope.
func (r *Repository) ListOutbox(ctx context.Context, state domain.OutboxState, limit int) ([]domain.OutboxEntry, error) {
	rows, err := r.pool.Query(ctx, selectOutboxQuery, string(state), limit)
	if err != nil {
		return nil, fmt.Errorf("listar outbox: %w", err)
	}
	defer rows.Close()
	entries := []domain.OutboxEntry{}
	for rows.Next() {
		var entry domain.OutboxEntry
		if err := rows.Scan(&entry.ID, &entry.JobID, &entry.RoutingKey, &entry.CreatedAt, &entry.PublishedAt); err != nil {
			return nil, fmt.Errorf("ler outbox: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// Ping executa SELECT 1; falha se o banco não responde. Usado pelo /healthz.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// scanJob lê uma linha de jobs (de QueryRow ou Rows) para o modelo de domínio.
func scanJob(row pgx.Row) (domain.JobRecord, error) {
	var record domain.JobRecord
	var jobType, target, origin string
	if err := row.Scan(&record.JobID, &jobType, &target, &origin, &record.CreatedAt); err != nil {
		return domain.JobRecord{}, err
	}
	record.Type, record.Target, record.Origin = domain.JobType(jobType), domain.Target(target), domain.Origin(origin)
	return record, nil
}

// rawJSON converte a coluna jsonb (lida como texto, ou NULL) em json.RawMessage.
func rawJSON(raw *string) json.RawMessage {
	if raw == nil {
		return nil
	}
	return json.RawMessage(*raw)
}
