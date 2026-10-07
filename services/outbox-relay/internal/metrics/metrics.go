// Package metrics define as métricas Prometheus do relay.
//
// Elas respondem às perguntas do benchmark: a fila está esvaziando (Pending), quanto tempo o
// job espera no banco antes de ir ao broker (PublishLag) e o relay está falhando (PublishErrors).
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics agrupa os coletores registrados pelo relay.
type Metrics struct {
	// Pending é o número de linhas da outbox ainda não publicadas.
	Pending prometheus.Gauge
	// PublishLag mede, em segundos, o tempo entre a criação da linha e a publicação confirmada.
	PublishLag prometheus.Histogram
	// Published conta mensagens publicadas e confirmadas pelo broker.
	Published prometheus.Counter
	// PublishErrors conta falhas de lote (banco, conexão com o broker, nack ou mensagem sem rota).
	PublishErrors prometheus.Counter
}

// New cria os coletores e os registra em reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending",
			Help: "Linhas da outbox aguardando publicação.",
		}),
		PublishLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_publish_lag_seconds",
			Help:    "Tempo entre a criação da linha na outbox e a publicação confirmada.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		Published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Mensagens publicadas e confirmadas pelo broker.",
		}),
		PublishErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_errors_total",
			Help: "Falhas ao processar um lote da outbox.",
		}),
	}
	reg.MustRegister(m.Pending, m.PublishLag, m.Published, m.PublishErrors)
	return m
}
