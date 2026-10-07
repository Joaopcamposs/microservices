// Package postgres implementa o acesso à tabela outbox com pgx.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/outbox-relay/internal/outbox"
)

// claimQuery reserva as linhas pendentes mais antigas.
//
// FOR UPDATE SKIP LOCKED faz réplicas do relay pularem linhas já reservadas por outra
// transação, então várias réplicas não publicam a mesma linha ao mesmo tempo. O envelope
// sai como texto para ser publicado como está no banco, sem remontar o JSON. O índice parcial
// outbox_pending_idx cobre este filtro.
const claimQuery = `
SELECT id, job_id::text, routing_key, envelope::text, created_at
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

// markPublishedQuery marca como publicadas as linhas confirmadas pelo broker.
const markPublishedQuery = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`

// countPendingQuery conta as pendentes; usa o índice parcial.
const countPendingQuery = `SELECT count(*) FROM outbox WHERE published_at IS NULL`

// purgeQuery apaga linhas publicadas além da retenção.
const purgeQuery = `DELETE FROM outbox WHERE published_at < now() - make_interval(secs => $1)`

// Store acessa a outbox no Postgres. Implementa outbox.Store e outbox.MaintenanceStore.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore cria o Store sobre um pool de conexões já aberto; quem o abre também o fecha.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// WithBatch reserva um lote, o entrega a publish e marca como publicados só os IDs devolvidos.
//
// Tudo ocorre em uma transação, e o COMMIT vem depois do confirm do broker (feito dentro de
// publish). Se o processo cair entre o publish e o commit, a transação reverte e as linhas
// voltam a ser pendentes: a mensagem pode ser republicada, nunca perdida (at-least-once).
// Se publish devolve IDs parciais com erro, os parciais são marcados e comitados para não
// serem republicados, e o erro é devolvido ao chamador.
func (s *Store) WithBatch(ctx context.Context, limit int, publish outbox.PublishFunc) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("iniciar transação: %w", err)
	}
	// Após o Commit o Rollback é um no-op; cobre todos os retornos antecipados.
	defer func() { _ = tx.Rollback(ctx) }()

	messages, err := claimBatch(ctx, tx, limit)
	if err != nil {
		return 0, err
	}
	if len(messages) == 0 {
		return 0, nil
	}

	publishedIDs, publishErr := publish(ctx, messages)

	if len(publishedIDs) > 0 {
		if _, err := tx.Exec(ctx, markPublishedQuery, publishedIDs); err != nil {
			return 0, fmt.Errorf("marcar como publicadas: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit: %w", err)
		}
	}
	return len(publishedIDs), publishErr
}

// claimBatch executa claimQuery dentro de tx e converte as linhas em outbox.Message.
func claimBatch(ctx context.Context, tx pgx.Tx, limit int) ([]outbox.Message, error) {
	rows, err := tx.Query(ctx, claimQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("reservar lote: %w", err)
	}
	defer rows.Close()

	var messages []outbox.Message
	for rows.Next() {
		var message outbox.Message
		var envelope string
		if err := rows.Scan(&message.ID, &message.JobID, &message.RoutingKey, &envelope, &message.CreatedAt); err != nil {
			return nil, fmt.Errorf("ler linha da outbox: %w", err)
		}
		message.Envelope = []byte(envelope)
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterar lote: %w", err)
	}
	return messages, nil
}

// CountPending devolve quantas linhas aguardam publicação.
func (s *Store) CountPending(ctx context.Context) (int64, error) {
	var pending int64
	if err := s.pool.QueryRow(ctx, countPendingQuery).Scan(&pending); err != nil {
		return 0, fmt.Errorf("contar pendentes: %w", err)
	}
	return pending, nil
}

// PurgePublished apaga linhas publicadas há mais de olderThan e devolve quantas removeu.
func (s *Store) PurgePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, purgeQuery, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("purgar outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}
