// Package metrics define as métricas Prometheus do worker, num registry próprio (injetado)
// para não depender de estado global.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics reúne o contador de jobs por status e o histograma de duração do handler. Os nomes
// são idênticos aos do worker-asyncio para o benchmark comparar as stacks no mesmo painel.
type Metrics struct {
	// Processed conta jobs por status: succeeded, failed, duplicate ou rejected.
	Processed *prometheus.CounterVec
	// Duration mede só o handler (sem fila nem banco): separa o custo do trabalho do framework.
	Duration prometheus.Histogram
}

// New cria e registra as métricas no registry recebido.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_jobs_processed_total",
			Help: "Jobs processados, por status (succeeded, failed, duplicate, rejected).",
		}, []string{"status"}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "worker_job_duration_seconds",
			Help: "Duração do handler.",
		}),
	}
	reg.MustRegister(m.Processed, m.Duration)
	return m
}
