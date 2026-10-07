package config

import (
	"testing"
	"time"
)

// envFrom devolve um getenv que lê de um mapa, no lugar do ambiente real.
func envFrom(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

// TestLoadUsesDefaults garante que o relay sobe sem nenhuma variável (fluxo `make up`).
func TestLoadUsesDefaults(t *testing.T) {
	cfg, err := Load(envFrom(nil))
	if err != nil {
		t.Fatalf("Load() erro inesperado: %v", err)
	}
	if cfg.PollInterval != 50*time.Millisecond || cfg.BatchSize != 100 || cfg.Retention != time.Hour {
		t.Errorf("padrões inesperados: %+v", cfg)
	}
}

// TestLoadReadsOverrides garante que as variáveis RELAY_* substituem os padrões.
func TestLoadReadsOverrides(t *testing.T) {
	cfg, err := Load(envFrom(map[string]string{
		"RELAY_POLL_INTERVAL": "200ms",
		"RELAY_BATCH_SIZE":    "10",
	}))
	if err != nil {
		t.Fatalf("Load() erro inesperado: %v", err)
	}
	if cfg.PollInterval != 200*time.Millisecond || cfg.BatchSize != 10 {
		t.Errorf("overrides não aplicados: %+v", cfg)
	}
}

// TestLoadRejectsInvalidValues protege contra lote fora do limite do publisher e valores mal formados.
func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := map[string]map[string]string{
		"lote acima do limite": {"RELAY_BATCH_SIZE": "5000"},
		"lote zero":            {"RELAY_BATCH_SIZE": "0"},
		"duração inválida":     {"RELAY_POLL_INTERVAL": "rapido"},
		"intervalo negativo":   {"RELAY_PURGE_INTERVAL": "-1s"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(envFrom(env)); err == nil {
				t.Errorf("esperava erro para %v", env)
			}
		})
	}
}
