// Command outbox-relay lê as linhas pendentes da tabela outbox e as publica no RabbitMQ.
//
// É o único processo que publica jobs: os gateways (Python e Go) só gravam na outbox, dentro
// da mesma transação que cria o job. Isso elimina a escrita dupla banco + broker.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"microservices-lab/outbox-relay/internal/config"
	"microservices-lab/outbox-relay/internal/metrics"
	"microservices-lab/outbox-relay/internal/outbox"
	"microservices-lab/outbox-relay/internal/postgres"
	"microservices-lab/outbox-relay/internal/rabbitmq"
)

// shutdownTimeout é o tempo máximo para o servidor de métricas encerrar.
const shutdownTimeout = 5 * time.Second

// main só repassa o erro de run para o código de saída; a lógica fica em run para que os
// defers (fechar pool e broker) executem antes de os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("relay encerrado com erro", "error", err)
		os.Exit(1)
	}
}

// run monta as dependências, sobe as goroutines auxiliares e roda o relay até SIGINT/SIGTERM.
func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("configuração: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("criar pool do Postgres: %w", err)
	}
	defer pool.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	relayMetrics := metrics.New(registry)

	store := postgres.NewStore(pool)
	publisher := rabbitmq.NewPublisher(cfg.AMQPURL, log)
	defer func() { _ = publisher.Close() }()

	relay := outbox.NewRelay(store, publisher, relayMetrics, cfg.BatchSize, cfg.PollInterval, log)
	maintainer := outbox.NewMaintainer(store, relayMetrics, cfg.Retention, cfg.PurgeInterval, log)
	server := newMetricsServer(cfg.MetricsAddr, registry)

	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); maintainer.Run(ctx) }()
	go func() { defer workers.Done(); serveMetrics(server, log) }()

	log.Info("relay iniciado", "batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String())
	relay.Run(ctx)

	log.Info("encerrando")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	workers.Wait()
	return nil
}

// newMetricsServer cria o servidor HTTP com /metrics (Prometheus) e /healthz (liveness).
func newMetricsServer(addr string, registry *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// serveMetrics bloqueia servindo HTTP; http.ErrServerClosed é o encerramento esperado.
func serveMetrics(server *http.Server, log *slog.Logger) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("servidor de métricas falhou", "error", err)
	}
}
