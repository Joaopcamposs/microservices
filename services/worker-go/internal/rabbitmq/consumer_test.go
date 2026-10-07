package rabbitmq

import (
	"errors"
	"fmt"
	"testing"

	"microservices-lab/worker-go/internal/domain"
)

// Decide é a regra de ack do projeto: inválida vai para a DLQ, infra ganha um requeue e, se já
// reentregue, vai para a DLQ (sem loop infinito).
func TestDecide(t *testing.T) {
	infra := errors.New("banco fora")
	invalid := fmt.Errorf("%w: payload", domain.ErrInvalidMessage)
	cases := []struct {
		name        string
		err         error
		redelivered bool
		want        Disposition
	}{
		{"sucesso", nil, false, Ack},
		{"sucesso reentregue", nil, true, Ack},
		{"inválida", invalid, false, RejectToDLQ},
		{"infra primeira vez", infra, false, Requeue},
		{"infra reentregue", infra, true, RejectToDLQ},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decide(tc.err, tc.redelivered); got != tc.want {
				t.Fatalf("Decide = %v, esperado %v", got, tc.want)
			}
		})
	}
}
