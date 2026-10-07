// Command gateway-go é a API HTTP (Gin) que recebe jobs e os grava na outbox.
//
// Tem a mesma API e o mesmo comportamento do gateway-py: os dois são intercambiáveis. Nunca
// publica no RabbitMQ; quem publica é o outbox-relay.
//
//	@title			gateway-go
//	@version		0.1.0
//	@description	Gateway do Microservices Lab em Go (Gin). A UI de teste é esta página (`/docs`).
//	@BasePath		/
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

	"microservices-lab/gateway-go/internal/api"
	"microservices-lab/gateway-go/internal/config"
	"microservices-lab/gateway-go/internal/domain"
	"microservices-lab/gateway-go/internal/postgres"
	"microservices-lab/gateway-go/internal/service"
	"microservices-lab/gateway-go/internal/tracing"

	// Importa o pacote gerado pelo `swag init`: ele registra o spec lido pela Swagger UI.
	_ "microservices-lab/gateway-go/docs"
)

// shutdownTimeout é o tempo máximo para terminar as requisições em voo no encerramento.
const shutdownTimeout = 5 * time.Second

// main só repassa o erro de run para o código de saída; a lógica fica em run para que os
// defers (fechar o pool) executem antes de os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("gateway encerrado com erro", "error", err)
		os.Exit(1)
	}
}

// run monta as dependências, sobe o servidor e o encerra de forma ordenada em SIGINT/SIGTERM.
func run() error {
	cfg := config.Load(os.Getenv)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	validator, err := domain.NewPayloadValidator(cfg.ContractsDir + "/jobs")
	if err != nil {
		return fmt.Errorf("carregar schemas: %w", err)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("criar pool: %w", err)
	}
	defer pool.Close()

	tracerProvider, err := tracing.NewProvider(ctx, "gateway-go")
	if err != nil {
		return fmt.Errorf("configurar traces: %w", err)
	}
	// Descarrega os spans pendentes no encerramento, com contexto novo (ctx já foi cancelado).
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := tracerProvider.Shutdown(flushCtx); err != nil {
			slog.Error("descarregar traces", "error", err)
		}
	}()

	jobs := service.NewJobService(postgres.NewRepository(pool), validator, tracerProvider.Tracer("gateway-go"), time.Now)
	router := api.NewRouter(api.NewHandler(jobs), tracerProvider)
	server := &http.Server{Addr: cfg.Addr, Handler: router, ReadHeaderTimeout: 5 * time.Second}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	slog.Info("gateway-go iniciado", "addr", cfg.Addr)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("encerrar servidor: %w", err)
	}
	return nil
}
