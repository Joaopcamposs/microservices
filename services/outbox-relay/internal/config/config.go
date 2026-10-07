// Package config carrega a configuração do relay a partir de variáveis de ambiente.
//
// Manter a configuração fora do código permite rodar o mesmo binário localmente e no Docker
// mudando só o ambiente.
package config

import (
	"fmt"
	"strconv"
	"time"
)

// maxBatchSize limita o lote porque o publisher bufferiza os retornos (basic.return) do
// broker em um canal de 1024 posições; um lote maior poderia bloquear a leitura do canal.
const maxBatchSize = 1000

// Config reúne os parâmetros do relay. Todos têm padrão para desenvolvimento com `make up`.
type Config struct {
	// DatabaseURL é a conexão com o Postgres que contém a tabela outbox.
	DatabaseURL string
	// AMQPURL é a conexão com o RabbitMQ.
	AMQPURL string
	// PollInterval é a espera quando não há linhas pendentes. Entra na latência ponta a ponta.
	PollInterval time.Duration
	// BatchSize é o máximo de linhas lidas e publicadas por transação.
	BatchSize int
	// Retention é por quanto tempo linhas já publicadas ficam na tabela antes da purga.
	Retention time.Duration
	// PurgeInterval é o intervalo entre execuções da purga.
	PurgeInterval time.Duration
	// MetricsAddr é o endereço do servidor HTTP de /metrics e /healthz.
	MetricsAddr string
}

// Load lê as variáveis `RELAY_*` usando getenv (injetado para facilitar os testes) e valida
// os valores. Variável ausente ou vazia usa o padrão.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:   envOr(getenv, "RELAY_DATABASE_URL", "postgres://postgres:lab@localhost:5432/lab"),
		AMQPURL:       envOr(getenv, "RELAY_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		MetricsAddr:   envOr(getenv, "RELAY_METRICS_ADDR", ":9100"),
		PollInterval:  50 * time.Millisecond,
		BatchSize:     100,
		Retention:     time.Hour,
		PurgeInterval: time.Minute,
	}

	var err error
	if cfg.PollInterval, err = durationOr(getenv, "RELAY_POLL_INTERVAL", cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if cfg.Retention, err = durationOr(getenv, "RELAY_RETENTION", cfg.Retention); err != nil {
		return Config{}, err
	}
	if cfg.PurgeInterval, err = durationOr(getenv, "RELAY_PURGE_INTERVAL", cfg.PurgeInterval); err != nil {
		return Config{}, err
	}
	if cfg.BatchSize, err = intOr(getenv, "RELAY_BATCH_SIZE", cfg.BatchSize); err != nil {
		return Config{}, err
	}

	if cfg.BatchSize < 1 || cfg.BatchSize > maxBatchSize {
		return Config{}, fmt.Errorf("RELAY_BATCH_SIZE deve estar entre 1 e %d, recebi %d", maxBatchSize, cfg.BatchSize)
	}
	if cfg.PollInterval <= 0 || cfg.PurgeInterval <= 0 {
		return Config{}, fmt.Errorf("RELAY_POLL_INTERVAL e RELAY_PURGE_INTERVAL devem ser positivos")
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

// durationOr interpreta a variável como time.Duration ("50ms", "1h") ou devolve o padrão.
func durationOr(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s inválida: %w", key, err)
	}
	return value, nil
}

// intOr interpreta a variável como inteiro ou devolve o padrão.
func intOr(getenv func(string) string, key string, fallback int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s inválida: %w", key, err)
	}
	return value, nil
}
