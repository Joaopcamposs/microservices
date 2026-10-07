package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"microservices-lab/gateway-go/internal/domain"
	"microservices-lab/gateway-go/internal/service"
)

// stubStore devolve dados fixos; registra o que foi enfileirado.
type stubStore struct {
	view     *domain.JobView
	enqueued int
	outbox   domain.OutboxState
}

func (s *stubStore) Enqueue(context.Context, domain.Envelope, domain.Target, time.Time) error {
	s.enqueued++
	return nil
}
func (s *stubStore) Find(context.Context, string) (*domain.JobView, error) { return s.view, nil }
func (s *stubStore) ListRecent(context.Context, int) ([]domain.JobView, error) {
	return []domain.JobView{}, nil
}
func (s *stubStore) ListOutbox(_ context.Context, state domain.OutboxState, _ int) ([]domain.OutboxEntry, error) {
	s.outbox = state
	return []domain.OutboxEntry{}, nil
}
func (s *stubStore) Ping(context.Context) error { return nil }

func newTestRouter(t *testing.T, store *stubStore) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	validator, err := domain.NewPayloadValidator("../../../../contracts/jobs")
	if err != nil {
		t.Fatal(err)
	}
	return NewRouter(NewHandler(service.NewJobService(store, validator, time.Now)))
}

func do(router *gin.Engine, method, target, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(method, target, strings.NewReader(body)))
	return recorder
}

// TestSubmitJobStatusCodes garante o contrato HTTP idêntico ao gateway-py: 202 válido e 422
// para tipo/target/payload inválidos, sem gravar nada nos casos de erro.
func TestSubmitJobStatusCodes(t *testing.T) {
	cases := []struct {
		name         string
		target, body string
		want         int
		wantEnqueued int
	}{
		{"válido", "/jobs?type=io.sleep&target=go", `{"ms": 10}`, 202, 1},
		{"target padrão é all", "/jobs?type=io.sleep", `{"ms": 10}`, 202, 1},
		{"payload inválido", "/jobs?type=io.sleep", `{"ms": -1}`, 422, 0},
		{"tipo desconhecido", "/jobs?type=nada", `{}`, 422, 0},
		{"tipo ausente", "/jobs", `{"ms": 10}`, 422, 0},
		{"target inválido", "/jobs?type=io.sleep&target=java", `{"ms": 10}`, 422, 0},
		{"tipo sem schema", "/jobs?type=cpu.pbkdf2", `{}`, 422, 0},
		{"corpo vazio", "/jobs?type=io.sleep", ``, 422, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &stubStore{}
			response := do(newTestRouter(t, store), http.MethodPost, tc.target, tc.body)
			if response.Code != tc.want || store.enqueued != tc.wantEnqueued {
				t.Errorf("status=%d enqueued=%d; esperava %d/%d (%s)",
					response.Code, store.enqueued, tc.want, tc.wantEnqueued, response.Body)
			}
		})
	}
}

// TestGetJob garante 404 para job inexistente, 422 para id malformado e 200 com status derivado.
func TestGetJob(t *testing.T) {
	const id = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	found := &domain.JobView{Record: domain.JobRecord{JobID: id, Type: domain.JobIOSleep, Target: domain.TargetGo}}

	if code := do(newTestRouter(t, &stubStore{}), http.MethodGet, "/jobs/"+id, "").Code; code != 404 {
		t.Errorf("inexistente: %d", code)
	}
	if code := do(newTestRouter(t, &stubStore{}), http.MethodGet, "/jobs/xyz", "").Code; code != 422 {
		t.Errorf("id inválido: %d", code)
	}
	response := do(newTestRouter(t, &stubStore{view: found}), http.MethodGet, "/jobs/"+id, "")
	var body JobOut
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 200 {
		t.Fatalf("status=%d err=%v", response.Code, err)
	}
	if body.Status != "pending" || body.Results == nil {
		t.Errorf("esperava pending e results []: %+v", body)
	}
}

// TestListsReturnEmptyArrayNotNull protege clientes e o Swagger: lista vazia é [], nunca null.
func TestListsReturnEmptyArrayNotNull(t *testing.T) {
	for _, path := range []string{"/jobs", "/outbox"} {
		response := do(newTestRouter(t, &stubStore{}), http.MethodGet, path, "")
		if strings.TrimSpace(response.Body.String()) != "[]" {
			t.Errorf("%s: corpo = %q", path, response.Body)
		}
	}
}

// TestOutboxFilter garante que o filtro chega ao store e que valores inválidos dão 422.
func TestOutboxFilter(t *testing.T) {
	store := &stubStore{}
	do(newTestRouter(t, store), http.MethodGet, "/outbox?state=published", "")
	if store.outbox != domain.OutboxPublished {
		t.Errorf("estado repassado = %q", store.outbox)
	}
	if code := do(newTestRouter(t, store), http.MethodGet, "/outbox?state=x", "").Code; code != 422 {
		t.Errorf("state inválido: %d", code)
	}
	if code := do(newTestRouter(t, store), http.MethodGet, "/jobs?limit=0", "").Code; code != 422 {
		t.Errorf("limit inválido: %d", code)
	}
}

// TestSwaggerIsServedAtDocs garante a UI e o spec em /docs (mesmo endereço do gateway-py) e
// que /docs e /swagger sem sufixo redirecionam para a UI.
func TestSwaggerIsServedAtDocs(t *testing.T) {
	router := newTestRouter(t, &stubStore{})
	for _, path := range []string{"/docs/index.html", "/docs/doc.json"} {
		if code := do(router, http.MethodGet, path, "").Code; code != 200 {
			t.Errorf("%s: %d", path, code)
		}
	}
	for _, path := range []string{"/docs", "/swagger"} {
		response := do(router, http.MethodGet, path, "")
		if response.Code != 302 || response.Header().Get("Location") != "/docs/index.html" {
			t.Errorf("%s: %d -> %s", path, response.Code, response.Header().Get("Location"))
		}
	}
}
