package domain

import (
	"context"
	"testing"
	"time"
)

func TestIOSleepReturnsSleptMs(t *testing.T) {
	out, err := NewHandlers()[JobIOSleep](context.Background(), []byte(`{"ms":1}`))
	if err != nil || string(out) != `{"slept_ms": 1}` {
		t.Fatalf("out=%s err=%v", out, err)
	}
}

// Cancelar o contexto interrompe o sleep em vez de segurar o encerramento.
func TestIOSleepRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := NewHandlers()[JobIOSleep](ctx, []byte(`{"ms":5000}`)); err == nil {
		t.Fatal("esperava erro de contexto")
	}
	if time.Since(start) > time.Second {
		t.Fatal("não respeitou o cancelamento")
	}
}
