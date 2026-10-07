// Command mock-server sobe o servidor de mock do io.fetch_urls (porta 8090 por padrão).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"microservices-lab/mock-server/internal/server"
)

// shutdownTimeout é o prazo para as requisições em voo terminarem no encerramento.
const shutdownTimeout = 5 * time.Second

// main sobe o servidor e o encerra com graceful shutdown em SIGINT/SIGTERM.
func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	srv := &http.Server{Addr: addr, Handler: server.New(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("encerrar servidor", "error", err)
		}
	}()

	slog.Info("mock-server no ar", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("servidor falhou", "error", err)
		os.Exit(1)
	}
}
