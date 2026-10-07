// Package outbox contém a lógica do relay: ler linhas pendentes da outbox e publicá-las.
//
// O pacote só define interfaces para o banco e o broker (Store e Publisher), então a regra
// central é testável sem infraestrutura. As implementações ficam em internal/postgres e
// internal/rabbitmq.
package outbox

import "time"

// Message é uma linha pendente da tabela outbox.
type Message struct {
	// ID é a chave da linha na outbox; é com ele que o relay marca a linha como publicada.
	ID int64
	// JobID é o identificador do job, usado como MessageId no AMQP.
	JobID string
	// RoutingKey vazia significa fanout (target=all); preenchida, routing key do exchange direct.
	RoutingKey string
	// Envelope é o JSON do contrato, publicado sem reinterpretação dos campos.
	Envelope []byte
	// CreatedAt é quando a linha entrou na outbox; base da métrica de lag.
	CreatedAt time.Time
}
