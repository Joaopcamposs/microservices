// Package config carrega a configuração do worker a partir de variáveis `WORKER_*`, para o
// mesmo binário rodar local e no Docker mudando só o ambiente.
package config

import (
	"fmt"
	"path/filepath"
	"strconv"
)

// Config reúne os parâmetros do worker. Todos têm padrão para desenvolvimento com `make up`.
type Config struct {
	// DatabaseURL é a conexão com o Postgres (formato pgx).
	DatabaseURL string
	// AMQPURL é a conexão com o RabbitMQ.
	AMQPURL string
	// Queue é a fila própria desta stack; o binding vem de infra/rabbitmq/definitions.json.
	Queue string
	// Prefetch é o máximo de mensagens entregues sem ack ao mesmo tempo. Igual nas quatro
	// stacks (benchmark justo).
	Prefetch int
	// PoolSize é o número de goroutines processando em paralelo. Padrão igual ao prefetch:
	// com menos goroutines que prefetch, um job I/O-bound ficaria esperando vaga mesmo já
	// tendo sido entregue, e a comparação com o worker asyncio (concorrência = prefetch) não
	// seria justa.
	PoolSize int
	// MetricsAddr é o endereço do servidor HTTP de /metrics.
	MetricsAddr string
	// ContractsDir é a pasta com os schemas compartilhados entre os serviços.
	ContractsDir string
}

// Load lê as variáveis usando getenv (injetado para facilitar testes) e valida os números.
// Variável ausente ou vazia usa o padrão.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:  envOr(getenv, "WORKER_DATABASE_URL", "postgres://postgres:lab@localhost:5432/lab"),
		AMQPURL:      envOr(getenv, "WORKER_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		Queue:        envOr(getenv, "WORKER_QUEUE", "jobs.go"),
		MetricsAddr:  envOr(getenv, "WORKER_METRICS_ADDR", ":9102"),
		ContractsDir: envOr(getenv, "WORKER_CONTRACTS_DIR", filepath.Join("..", "..", "contracts")),
		Prefetch:     64,
	}
	var err error
	if cfg.Prefetch, err = positiveInt(getenv, "WORKER_PREFETCH", cfg.Prefetch); err != nil {
		return Config{}, err
	}
	if cfg.PoolSize, err = positiveInt(getenv, "WORKER_POOL_SIZE", cfg.Prefetch); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// envOr devolve o valor da variável ou o padrão quando ela está vazia.
func envOr(getenv func(string) string, key, fallback string) string {
	if value := getenv(key); value != "" {
		return value
	}
	return fallback
}

// positiveInt lê um inteiro maior que zero ou devolve o padrão se a variável está vazia.
func positiveInt(getenv func(string) string, key string, fallback int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s deve ser um inteiro maior que zero, recebi %q", key, raw)
	}
	return value, nil
}
