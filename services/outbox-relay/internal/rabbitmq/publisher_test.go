package rabbitmq

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"microservices-lab/outbox-relay/internal/outbox"
)

// Testes de integração: usam o RabbitMQ do `make up` (com a topologia de definitions.json) e
// são pulados se ele não responder. Usam uma fila temporária ligada a uma routing key única
// do exchange direct, para não tocar nas filas reais dos workers.

func amqpURL() string {
	if url := os.Getenv("RELAY_AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@localhost:5672/"
}

// bindTempQueue cria uma fila exclusiva ligada a routingKey no exchange jobs.direct.
func bindTempQueue(t *testing.T, routingKey string) <-chan amqp.Delivery {
	t.Helper()
	conn, err := amqp.Dial(amqpURL())
	if err != nil {
		t.Skipf("RabbitMQ indisponível (rode `make up`): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.QueueBind(queue.Name, routingKey, directExchange, false, nil); err != nil {
		t.Fatal(err)
	}
	deliveries, err := channel.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return deliveries
}

func newTestPublisher(t *testing.T) *Publisher {
	t.Helper()
	publisher := NewPublisher(amqpURL(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = publisher.Close() })
	return publisher
}

// TestPublishDeliversEnvelopeUnchanged garante que o corpo e o MessageId chegam ao consumidor
// exatamente como estão na outbox: o relay não reinterpreta o envelope.
func TestPublishDeliversEnvelopeUnchanged(t *testing.T) {
	routingKey := "test-" + time.Now().Format("150405.000000")
	deliveries := bindTempQueue(t, routingKey)
	publisher := newTestPublisher(t)
	body := []byte(`{"job_id": "abc", "type": "io.sleep"}`)

	ids, err := publisher.Publish(context.Background(), []outbox.Message{
		{ID: 7, JobID: "abc", RoutingKey: routingKey, Envelope: body, CreatedAt: time.Now()},
	})

	if err != nil || len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("Publish() = %v, %v; esperava [7] sem erro", ids, err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != string(body) || delivery.MessageId != "abc" {
			t.Errorf("entrega alterada: body=%s messageId=%s", delivery.Body, delivery.MessageId)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Error("a mensagem devia ser persistente")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a mensagem não chegou à fila")
	}
}

// TestPublishDoesNotConfirmUnroutableMessage garante que mensagem sem rota NÃO conta como
// publicada: sem o flag mandatory o broker daria ack e o relay a marcaria como enviada,
// perdendo o job em silêncio.
func TestPublishDoesNotConfirmUnroutableMessage(t *testing.T) {
	if conn, err := amqp.Dial(amqpURL()); err != nil {
		t.Skipf("RabbitMQ indisponível (rode `make up`): %v", err)
	} else {
		_ = conn.Close()
	}
	publisher := newTestPublisher(t)

	ids, err := publisher.Publish(context.Background(), []outbox.Message{
		{ID: 9, JobID: "sem-rota", RoutingKey: "stack-que-nao-existe", Envelope: []byte(`{}`), CreatedAt: time.Now()},
	})

	if err == nil {
		t.Error("esperava erro de mensagem sem rota")
	}
	if len(ids) != 0 {
		t.Errorf("mensagem sem rota não pode ser confirmada: %v", ids)
	}
}
