package domain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Os vetores dourados de contracts/jobs/examples.json valem para as quatro stacks: o resultado
// do Go tem de ser igual ao do Python, campo a campo.
func TestHandlersMatchGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "examples.json"))
	if err != nil {
		t.Fatalf("ler vetores: %v", err)
	}
	var file struct {
		Examples []struct {
			Type     JobType         `json:"type"`
			Payload  json.RawMessage `json:"payload"`
			Expected json.RawMessage `json:"expected"`
		} `json:"examples"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("interpretar vetores: %v", err)
	}
	for _, ex := range file.Examples {
		out, err := NewHandlers()[ex.Type](context.Background(), ex.Payload)
		if err != nil {
			t.Fatalf("%s %s: %v", ex.Type, ex.Payload, err)
		}
		var got, want any
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(ex.Expected, &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s %s:\n got  %s\n want %s", ex.Type, ex.Payload, out, ex.Expected)
		}
	}
}

// Resultado na ordem recebida, com status e tamanho do corpo de cada URL.
func TestFetchURLsKeepsOrderAndReportsStatusAndSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			_, _ = w.Write([]byte("hello"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("nop"))
	}))
	defer srv.Close()
	payload, _ := json.Marshal(map[string]any{"urls": []string{srv.URL + "/missing", srv.URL + "/ok"}})
	out, err := NewHandlers()[JobIOFetchURLs](context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"results":[{"url":"` + srv.URL + `/missing","status":404,"bytes":3},{"url":"` + srv.URL + `/ok","status":200,"bytes":5}]}`
	if string(out) != want {
		t.Fatalf("got %s\nwant %s", out, want)
	}
}

// Erro de rede derruba o job inteiro (vira failed no processor).
func TestFetchURLsFailsWhenConnectionIsRefused(t *testing.T) {
	_, err := NewHandlers()[JobIOFetchURLs](context.Background(), []byte(`{"urls":["http://127.0.0.1:1/x"]}`))
	if err == nil {
		t.Fatal("esperava erro de conexão")
	}
}
