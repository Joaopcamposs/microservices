package outbox

import (
	"context"
	"log/slog"
	"time"

	"microservices-lab/outbox-relay/internal/metrics"
)

// tickTimeout limita um ciclo de leitura, publicação e commit. Um ciclo em andamento não é
// cancelado pelo shutdown (ver Run), então este teto evita ficar preso se o banco travar.
const tickTimeout = 30 * time.Second

// errorBackoff é a espera depois de uma falha, para não martelar um banco ou broker fora do ar.
const errorBackoff = time.Second

// PublishFunc publica um lote e devolve os IDs confirmados pelo broker.
//
// Contrato: pode devolver IDs parciais junto com um erro. Os IDs devolvidos foram
// publicados e confirmados; os demais devem continuar pendentes.
type PublishFunc func(ctx context.Context, messages []Message) (publishedIDs []int64, err error)

// Store é a porta de acesso à outbox no banco.
type Store interface {
	// WithBatch reserva até limit linhas pendentes, chama publish e marca como publicadas
	// apenas as linhas cujos IDs publish devolveu, tudo numa transação. Devolve quantas
	// linhas foram marcadas. Se publish falha, o erro é propagado depois de confirmar os IDs
	// parciais.
	WithBatch(ctx context.Context, limit int, publish PublishFunc) (int, error)
}

// Publisher entrega mensagens ao broker com confirmação.
type Publisher interface {
	// Publish envia o lote e devolve os IDs confirmados (ver PublishFunc).
	Publish(ctx context.Context, messages []Message) ([]int64, error)
}

// Relay esvazia a outbox para o broker. Garante entrega at-least-once: uma linha só é
// marcada como publicada depois do confirm do broker, então uma queda no meio pode republicar.
type Relay struct {
	store        Store
	publisher    Publisher
	metrics      *metrics.Metrics
	batchSize    int
	pollInterval time.Duration
	log          *slog.Logger
	now          func() time.Time
}

// NewRelay monta o relay com suas dependências injetadas.
func NewRelay(
	store Store,
	publisher Publisher,
	m *metrics.Metrics,
	batchSize int,
	pollInterval time.Duration,
	log *slog.Logger,
) *Relay {
	return &Relay{
		store:        store,
		publisher:    publisher,
		metrics:      m,
		batchSize:    batchSize,
		pollInterval: pollInterval,
		log:          log,
		now:          time.Now,
	}
}

// Tick processa um lote e devolve quantas linhas foram publicadas e marcadas.
func (r *Relay) Tick(ctx context.Context) (int, error) {
	return r.store.WithBatch(ctx, r.batchSize, r.publishAndMeasure)
}

// publishAndMeasure delega ao Publisher e registra o lag das mensagens confirmadas.
//
// O lag é medido aqui, e não no publisher, porque só o relay conhece o CreatedAt da linha.
func (r *Relay) publishAndMeasure(ctx context.Context, messages []Message) ([]int64, error) {
	ids, err := r.publisher.Publish(ctx, messages)

	confirmed := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		confirmed[id] = struct{}{}
	}
	now := r.now()
	for _, message := range messages {
		if _, ok := confirmed[message.ID]; ok {
			r.metrics.PublishLag.Observe(now.Sub(message.CreatedAt).Seconds())
		}
	}
	r.metrics.Published.Add(float64(len(ids)))
	return ids, err
}

// Run repete Tick até ctx ser cancelado.
//
// Cada Tick usa um contexto separado do de shutdown (context.WithoutCancel): um SIGTERM
// interrompe o laço, mas o lote em andamento termina e comita, sem republicar à toa.
// Com lote cheio, o próximo Tick começa na hora; sem trabalho, espera pollInterval.
func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		published, err := r.runTick(ctx)
		switch {
		case err != nil:
			r.metrics.PublishErrors.Inc()
			r.log.Error("falha ao processar lote da outbox", "error", err, "published", published)
			sleep(ctx, errorBackoff)
		case published == 0:
			sleep(ctx, r.pollInterval)
		}
	}
}

// runTick executa um Tick sob o teto de tempo, isolado do cancelamento do shutdown.
func (r *Relay) runTick(ctx context.Context) (int, error) {
	tickCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tickTimeout)
	defer cancel()
	return r.Tick(tickCtx)
}

// sleep espera d ou o cancelamento de ctx, o que vier primeiro.
func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
