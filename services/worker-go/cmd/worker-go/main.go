// Command worker-go consome a fila jobs.go e grava o resultado em job_results.
//
// É a stack Go do comparativo: goroutines num pool, ack manual e a mesma regra de entrega dos
// outros workers (resultado gravado antes do ack, DLQ para mensagem inválida).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"microservices-lab/worker-go/internal/config"
	"microservices-lab/worker-go/internal/domain"
	"microservices-lab/worker-go/internal/metrics"
	"microservices-lab/worker-go/internal/postgres"
	"microservices-lab/worker-go/internal/rabbitmq"
	"microservices-lab/worker-go/internal/service"
	"microservices-lab/worker-go/internal/tracing"
)

// shutdownTimeout é o tempo máximo para o servidor de métricas encerrar.
const shutdownTimeout = 5 * time.Second

// main só repassa o erro de run para o código de saída; a lógica fica em run para que os
// defers (fechar o pool) executem antes de os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("worker encerrado com erro", "error", err)
		os.Exit(1)
	}
}

// run monta as dependências e consome até SIGINT/SIGTERM, encerrando de forma ordenada.
func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	validator, err := domain.NewContractValidator(cfg.ContractsDir)
	if err != nil {
		return fmt.Errorf("carregar contratos: %w", err)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("criar pool: %w", err)
	}
	defer pool.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(registry)

	tracerProvider, err := tracing.NewProvider(ctx, "worker-go")
	if err != nil {
		return fmt.Errorf("configurar traces: %w", err)
	}
	// Descarrega os spans pendentes no encerramento, com contexto novo (ctx já foi cancelado).
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := tracerProvider.Shutdown(flushCtx); err != nil {
			log.Error("descarregar traces", "error", err)
		}
	}()

	processor := service.NewJobProcessor(validator, domain.NewHandlers(), postgres.NewResultRepository(pool), m, tracerProvider.Tracer("worker-go"), log, time.Now)
	consumer := rabbitmq.NewConsumer(cfg.AMQPURL, cfg.Queue, cfg.Prefetch, cfg.PoolSize, processor, m, log)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	server := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("servidor de métricas", "error", err)
		}
	}()

	if err := consumer.Run(ctx); err != nil {
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
