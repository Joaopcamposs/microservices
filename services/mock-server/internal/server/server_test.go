package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func get(path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	New().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// Cada rota responde com o status e o corpo previstos; entradas fora da faixa dão 400.
func TestRoutes(t *testing.T) {
	cases := []struct {
		path string
		code int
		body string
	}{
		{"/delay/0", 200, `{"delay_ms":0}`},
		{"/delay/60001", 400, ""},
		{"/delay/abc", 400, ""},
		{"/status/404", 404, `{"status":404}`},
		{"/status/99", 400, ""},
		{"/healthz", 200, `{"status":"ok"}`},
	}
	for _, tc := range cases {
		rec := get(tc.path)
		if rec.Code != tc.code || (tc.body != "" && rec.Body.String() != tc.body) {
			t.Errorf("%s: %d %q; esperava %d %q", tc.path, rec.Code, rec.Body.String(), tc.code, tc.body)
		}
	}
}
