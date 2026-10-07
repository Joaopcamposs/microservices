package outbox

import (
	"context"
	"log/slog"
	"time"

	"microservices-lab/outbox-relay/internal/metrics"
)

// pendingGaugeInterval é a frequência de atualização do gauge outbox_pending. Contar a cada
// Tick (50 ms) seria custo desnecessário; uma amostra por segundo basta para o dashboard.
const pendingGaugeInterval = time.Second

// MaintenanceStore é a porta das tarefas de manutenção da outbox.
type MaintenanceStore interface {
	// CountPending devolve quantas linhas ainda não foram publicadas.
	CountPending(ctx context.Context) (int64, error)
	// PurgePublished apaga linhas publicadas há mais de olderThan e devolve quantas removeu.
	PurgePublished(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Maintainer executa a manutenção periódica: purga de linhas antigas e atualização do gauge.
//
// Sem a purga a tabela e o índice crescem sem limite e o polling degrada.
type Maintainer struct {
	store         MaintenanceStore
	metrics       *metrics.Metrics
	retention     time.Duration
	purgeInterval time.Duration
	log           *slog.Logger
}

// NewMaintainer monta o Maintainer com suas dependências injetadas.
func NewMaintainer(
	store MaintenanceStore,
	m *metrics.Metrics,
	retention, purgeInterval time.Duration,
	log *slog.Logger,
) *Maintainer {
	return &Maintainer{
		store:         store,
		metrics:       m,
		retention:     retention,
		purgeInterval: purgeInterval,
		log:           log,
	}
}

// Run atualiza o gauge e purga linhas antigas até ctx ser cancelado.
func (m *Maintainer) Run(ctx context.Context) {
	gaugeTicker := time.NewTicker(pendingGaugeInterval)
	defer gaugeTicker.Stop()
	purgeTicker := time.NewTicker(m.purgeInterval)
	defer purgeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gaugeTicker.C:
			m.updatePendingGauge(ctx)
		case <-purgeTicker.C:
			m.purge(ctx)
		}
	}
}

// updatePendingGauge lê a contagem de pendentes e publica no gauge.
func (m *Maintainer) updatePendingGauge(ctx context.Context) {
	pending, err := m.store.CountPending(ctx)
	if err != nil {
		m.log.Warn("falha ao contar linhas pendentes", "error", err)
		return
	}
	m.metrics.Pending.Set(float64(pending))
}

// purge apaga linhas publicadas além do período de retenção.
func (m *Maintainer) purge(ctx context.Context) {
	removed, err := m.store.PurgePublished(ctx, m.retention)
	if err != nil {
		m.log.Warn("falha na purga da outbox", "error", err)
		return
	}
	if removed > 0 {
		m.log.Info("outbox purgada", "removed", removed)
	}
}
