package config

import "testing"

// env monta um getenv a partir de um mapa.
func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

// Padrões: pool igual ao prefetch para manter a comparação justa com o worker asyncio.
func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue != "jobs.go" || cfg.Prefetch != 64 || cfg.PoolSize != 64 {
		t.Fatalf("padrões inesperados: %+v", cfg)
	}
}

// Pool acompanha o prefetch quando só o prefetch é alterado.
func TestLoadPoolFollowsPrefetch(t *testing.T) {
	cfg, err := Load(env(map[string]string{"WORKER_PREFETCH": "8"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PoolSize != 8 {
		t.Fatalf("pool = %d, esperado 8", cfg.PoolSize)
	}
}

// Valor inválido deve falhar na subida, não em runtime.
func TestLoadRejectsInvalidNumber(t *testing.T) {
	for _, raw := range []string{"0", "-1", "abc"} {
		if _, err := Load(env(map[string]string{"WORKER_PREFETCH": raw})); err == nil {
			t.Fatalf("esperava erro para %q", raw)
		}
	}
}
