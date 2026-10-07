package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"microservices-lab/outbox-relay/internal/outbox"
)

// Testes de integração: usam o Postgres do `make up` e são pulados se ele não responder.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("RELAY_DATABASE_URL")
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

// seedJobs cria n jobs com linha na outbox e devolve os IDs da outbox e os job_ids.
func seedJobs(t *testing.T, pool *pgxpool.Pool, n int) (outboxIDs []int64, jobIDs []string) {
	t.Helper()
	ctx := context.Background()
	for range n {
		jobID := uuid.NewString()
		if _, err := pool.Exec(ctx,
			`INSERT INTO jobs (job_id, type, target, origin) VALUES ($1, 'io.sleep', 'all', 'gateway-py')`, jobID); err != nil {
			t.Fatalf("inserir job: %v", err)
		}
		var id int64
		envelope := fmt.Sprintf(`{"job_id": "%s"}`, jobID)
		if err := pool.QueryRow(ctx,
			`INSERT INTO outbox (job_id, envelope) VALUES ($1, $2::jsonb) RETURNING id`, jobID, envelope).Scan(&id); err != nil {
			t.Fatalf("inserir outbox: %v", err)
		}
		outboxIDs = append(outboxIDs, id)
		jobIDs = append(jobIDs, jobID)
	}
	t.Cleanup(func() {
		for _, jobID := range jobIDs {
			_, _ = pool.Exec(ctx, `DELETE FROM outbox WHERE job_id = $1`, jobID)
			_, _ = pool.Exec(ctx, `DELETE FROM jobs WHERE job_id = $1`, jobID)
		}
	})
	return outboxIDs, jobIDs
}

// isPublished informa se a linha da outbox já tem published_at.
func isPublished(t *testing.T, pool *pgxpool.Pool, id int64) bool {
	t.Helper()
	var published bool
	if err := pool.QueryRow(context.Background(),
		`SELECT published_at IS NOT NULL FROM outbox WHERE id = $1`, id).Scan(&published); err != nil {
		t.Fatalf("consultar outbox: %v", err)
	}
	return published
}

// only filtra o lote para as linhas semeadas pelo teste, ignorando dados de outros usos do banco.
func only(messages []outbox.Message, ids []int64) []outbox.Message {
	var mine []outbox.Message
	for _, message := range messages {
		for _, id := range ids {
			if message.ID == id {
				mine = append(mine, message)
			}
		}
	}
	return mine
}

func idsOf(messages []outbox.Message) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	return ids
}

// TestWithBatchMarksOnlyConfirmedIDs garante que uma publicação parcial marca só o que o broker
// confirmou: marcar o resto perderia mensagens, e não marcar o parcial as duplicaria.
func TestWithBatchMarksOnlyConfirmedIDs(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ids, _ := seedJobs(t, pool, 2)

	_, err := store.WithBatch(context.Background(), 1000, func(_ context.Context, messages []outbox.Message) ([]int64, error) {
		mine := only(messages, ids)
		return idsOf(mine[:1]), fmt.Errorf("falha no segundo")
	})

	if err == nil {
		t.Fatal("esperava o erro de publish propagado")
	}
	if !isPublished(t, pool, ids[0]) || isPublished(t, pool, ids[1]) {
		t.Errorf("devia marcar só a primeira linha: %v / %v", isPublished(t, pool, ids[0]), isPublished(t, pool, ids[1]))
	}
}

// TestWithBatchLeavesRowsPendingWhenNothingConfirmed garante o at-least-once: sem confirm, a
// linha continua pendente para uma próxima tentativa.
func TestWithBatchLeavesRowsPendingWhenNothingConfirmed(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ids, _ := seedJobs(t, pool, 1)

	_, err := store.WithBatch(context.Background(), 1000, func(context.Context, []outbox.Message) ([]int64, error) {
		return nil, fmt.Errorf("broker fora")
	})

	if err == nil {
		t.Fatal("esperava erro")
	}
	if isPublished(t, pool, ids[0]) {
		t.Error("a linha não podia ser marcada sem confirm")
	}
}

// TestWithBatchSkipsRowsLockedByAnotherReplica garante o SKIP LOCKED: enquanto uma réplica
// segura uma linha, outra réplica não a recebe, então não há publicação duplicada simultânea.
func TestWithBatchSkipsRowsLockedByAnotherReplica(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ids, _ := seedJobs(t, pool, 1)

	holding := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.WithBatch(context.Background(), 1000, func(_ context.Context, messages []outbox.Message) ([]int64, error) {
			if len(only(messages, ids)) == 1 {
				close(holding)
				<-release
			}
			return nil, nil
		})
		firstDone <- err
	}()

	select {
	case <-holding:
	case <-time.After(3 * time.Second):
		t.Fatal("a primeira réplica não reservou a linha")
	}

	sawLockedRow := false
	_, err := store.WithBatch(context.Background(), 1000, func(_ context.Context, messages []outbox.Message) ([]int64, error) {
		sawLockedRow = len(only(messages, ids)) > 0
		return nil, nil
	})
	close(release)

	if err != nil {
		t.Fatalf("segunda réplica falhou: %v", err)
	}
	if sawLockedRow {
		t.Error("a segunda réplica recebeu uma linha que a primeira ainda segurava")
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("primeira réplica falhou: %v", err)
	}
}

// TestPurgePublishedRemovesOnlyOldPublishedRows garante que a purga preserva pendentes e
// publicadas recentes.
func TestPurgePublishedRemovesOnlyOldPublishedRows(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ids, _ := seedJobs(t, pool, 3)
	ctx := context.Background()
	// ids[0]: publicada há 2h (deve sumir); ids[1]: publicada agora; ids[2]: pendente.
	mustExec(t, pool, `UPDATE outbox SET published_at = now() - interval '2 hours' WHERE id = $1`, ids[0])
	mustExec(t, pool, `UPDATE outbox SET published_at = now() WHERE id = $1`, ids[1])

	if _, err := store.PurgePublished(ctx, time.Hour); err != nil {
		t.Fatalf("PurgePublished: %v", err)
	}

	for index, wantExists := range []bool{false, true, true} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM outbox WHERE id = $1)`, ids[index]).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != wantExists {
			t.Errorf("linha %d: existe=%v, esperava %v", index, exists, wantExists)
		}
	}
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}
