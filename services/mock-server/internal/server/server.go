// Package server implementa o mock HTTP usado pelo job io.fetch_urls: latência e status
// controlados, para o benchmark não depender da internet.
package server

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const (
	// maxDelayMs limita /delay para um pedido esquecido não segurar conexão por muito tempo.
	maxDelayMs = 60000
	// minStatus e maxStatus delimitam os códigos HTTP aceitos em /status.
	minStatus = 100
	maxStatus = 599
)

// Mock reúne as rotas do servidor de mock. Não guarda estado: cada resposta depende só do caminho.
type Mock struct{}

// New devolve o roteador com /delay/{ms}, /status/{code} e /healthz.
func New() http.Handler {
	mock := &Mock{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /delay/{ms}", mock.Delay)
	mux.HandleFunc("GET /status/{code}", mock.Status)
	mux.HandleFunc("GET /healthz", mock.Health)
	return mux
}

// Delay espera `ms` milissegundos e responde 200 com um corpo de tamanho fixo por valor. Encerra
// cedo se o cliente desistir, para não acumular goroutines paradas.
func (m *Mock) Delay(w http.ResponseWriter, r *http.Request) {
	ms, err := strconv.Atoi(r.PathValue("ms"))
	if err != nil || ms < 0 || ms > maxDelayMs {
		http.Error(w, fmt.Sprintf("ms deve ser inteiro entre 0 e %d", maxDelayMs), http.StatusBadRequest)
		return
	}
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
		return
	}
	writeBody(w, http.StatusOK, fmt.Sprintf(`{"delay_ms":%d}`, ms))
}

// Status responde imediatamente com o código pedido, para testar como cada stack trata 4xx/5xx.
func (m *Mock) Status(w http.ResponseWriter, r *http.Request) {
	code, err := strconv.Atoi(r.PathValue("code"))
	if err != nil || code < minStatus || code > maxStatus {
		http.Error(w, fmt.Sprintf("code deve ser inteiro entre %d e %d", minStatus, maxStatus), http.StatusBadRequest)
		return
	}
	writeBody(w, code, fmt.Sprintf(`{"status":%d}`, code))
}

// Health é o healthcheck do compose.
func (m *Mock) Health(w http.ResponseWriter, _ *http.Request) {
	writeBody(w, http.StatusOK, `{"status":"ok"}`)
}

// writeBody grava o status e um corpo JSON; centraliza o Content-Type e o tratamento de erro.
func writeBody(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// O cliente pode ter fechado a conexão; não há o que fazer com esse erro além de ignorar.
	_, _ = w.Write([]byte(body))
}
