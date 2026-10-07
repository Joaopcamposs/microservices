package domain

import (
	"errors"
	"testing"
)

// contractsJobsDir aponta para os schemas reais do monorepo (os mesmos usados em produção).
const contractsJobsDir = "../../../../contracts/jobs"

func newValidator(t *testing.T) *PayloadValidator {
	t.Helper()
	validator, err := NewPayloadValidator(contractsJobsDir)
	if err != nil {
		t.Fatalf("carregar schemas: %v", err)
	}
	return validator
}

// TestValidateAgainstRealSchema garante que o gateway Go aceita e rejeita o mesmo que o
// schema compartilhado define (a paridade com o gateway-py depende disso).
func TestValidateAgainstRealSchema(t *testing.T) {
	validator := newValidator(t)
	cases := []struct {
		name    string
		jobType JobType
		payload string
		wantErr error
	}{
		{"válido", JobIOSleep, `{"ms": 100}`, nil},
		{"inteiro com .0 é inteiro", JobIOSleep, `{"ms": 100.0}`, nil},
		{"abaixo do mínimo", JobIOSleep, `{"ms": -1}`, ErrInvalidPayload},
		{"acima do máximo", JobIOSleep, `{"ms": 60001}`, ErrInvalidPayload},
		{"decimal", JobIOSleep, `{"ms": 1.5}`, ErrInvalidPayload},
		{"campo obrigatório ausente", JobIOSleep, `{}`, ErrInvalidPayload},
		{"campo extra", JobIOSleep, `{"ms": 1, "x": 2}`, ErrInvalidPayload},
		{"não é objeto", JobIOSleep, `[1]`, ErrInvalidPayload},
		{"não é JSON", JobIOSleep, `{quebrado`, ErrInvalidPayload},
		{"tipo sem schema", JobPipelineFanout, `{}`, ErrUnsupportedJobType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.jobType, []byte(tc.payload))
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("erro = %v, esperava %v", err, tc.wantErr)
			}
		})
	}
}

// TestJobViewStatus garante o estado derivado: o fanout espera 4 resultados, o direct 1.
func TestJobViewStatus(t *testing.T) {
	results := func(n int) []WorkerResult { return make([]WorkerResult, n) }
	cases := []struct {
		name   string
		target Target
		count  int
		want   JobStatus
	}{
		{"sem resultados", TargetAll, 0, StatusPending},
		{"fanout parcial", TargetAll, 3, StatusRunning},
		{"fanout completo", TargetAll, 4, StatusCompleted},
		{"direct completo", TargetGo, 1, StatusCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view := JobView{Record: JobRecord{Target: tc.target}, Results: results(tc.count)}
			if got := view.Status(); got != tc.want {
				t.Errorf("Status() = %s, esperava %s", got, tc.want)
			}
		})
	}
}

// TestRoutingKey garante o roteamento da outbox: fanout usa chave vazia, direct usa a stack.
func TestRoutingKey(t *testing.T) {
	if TargetAll.RoutingKey() != "" || TargetGo.RoutingKey() != "go" {
		t.Errorf("routing keys erradas: %q / %q", TargetAll.RoutingKey(), TargetGo.RoutingKey())
	}
}
