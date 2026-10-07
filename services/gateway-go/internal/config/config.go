// Package config carrega a configuração do gateway a partir de variáveis de ambiente
// `GATEWAY_*`, para o mesmo binário rodar local e no Docker mudando só o ambiente.
package config

import "path/filepath"

// Config reúne os parâmetros do gateway. Todos têm padrão para desenvolvimento com `make up`.
type Config struct {
	// DatabaseURL é a conexão com o Postgres (formato pgx, sem o prefixo +asyncpg).
	DatabaseURL string
	// Addr é o endereço de escuta HTTP. Porta 8001 para conviver com o gateway-py na 8000.
	Addr string
	// ContractsDir é a pasta com os schemas compartilhados entre os serviços.
	ContractsDir string
}

// Load lê as variáveis usando getenv (injetado para facilitar testes). Ausente ou vazia usa o padrão.
func Load(getenv func(string) string) Config {
	return Config{
		DatabaseURL:  envOr(getenv, "GATEWAY_DATABASE_URL", "postgres://postgres:lab@localhost:5432/lab"),
		Addr:         envOr(getenv, "GATEWAY_ADDR", ":8001"),
		ContractsDir: envOr(getenv, "GATEWAY_CONTRACTS_DIR", filepath.Join("..", "..", "contracts")),
	}
}

// envOr devolve o valor da variável ou o padrão quando ela está vazia.
func envOr(getenv func(string) string, key, fallback string) string {
	if value := getenv(key); value != "" {
		return value
	}
	return fallback
}
