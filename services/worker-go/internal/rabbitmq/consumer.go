// Package rabbitmq liga a fila ao JobProcessor: consome com ack manual e decide ack, reject
// para a DLQ ou requeue.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"microservices-lab/worker-go/internal/domain"
	"microservices-lab/worker-go/internal/metrics"
)

// reconnectDelay é a espera entre tentativas de reconexão ao broker.
const reconnectDelay = time.Second

// Processor é o que o consumidor precisa do caso de uso (implementado por service.JobProcessor).
type Processor interface {
	// Process trata uma mensagem; erro de domain.ErrInvalidMessage significa "nunca vai passar".
	Process(ctx context.Context, body []byte) error
}

// Disposition é o que fazer com a mensagem depois do processamento.
type Disposition int

// Destinos possíveis de uma mensagem.
const (
	// Ack confirma a mensagem: o resultado já está gravado.
	Ack Disposition = iota
	// RejectToDLQ rejeita sem requeue: o broker a envia para a DLX/DLQ.
	RejectToDLQ
	// Requeue devolve a mensagem para uma segunda tentativa.
	Requeue
)

// Decide traduz o resultado do processamento em destino da mensagem.
//
// Mensagem inválida nunca vai passar, então vai direto para a DLQ. Falha de infraestrutura
// ganha uma segunda chance (cobre uma queda curta do banco); se a mensagem já foi reentregue,
// vai para a DLQ, evitando o requeue infinito que travaria a fila. É uma função pura para ser
// testada sem broker.
func Decide(err error, redelivered bool) Disposition {
	switch {
	case err == nil:
		return Ack
	case errors.Is(err, domain.ErrInvalidMessage):
		return RejectToDLQ
	case redelivered:
		return RejectToDLQ
	default:
		return Requeue
	}
}

// Consumer consome uma fila com um pool de goroutines e ack manual.
//
// Regra do projeto: o ack só acontece **depois** de o resultado estar gravado. Se o worker cair
// no meio, o broker reentrega (at-least-once) e a gravação idempotente absorve a duplicata.
type Consumer struct {
	url       string
	queue     string
	prefetch  int
	poolSize  int
	processor Processor
	metrics   *metrics.Metrics
	log       *slog.Logger
}

// NewConsumer guarda a configuração; a conexão só abre em Run.
func NewConsumer(url, queue string, prefetch, poolSize int, processor Processor, m *metrics.Metrics, log *slog.Logger) *Consumer {
	return &Consumer{url: url, queue: queue, prefetch: prefetch, poolSize: poolSize, processor: processor, metrics: m, log: log}
}

// Run consome até ctx ser cancelado, reconectando se o broker cair. Ao cancelar, para de
// receber mensagens novas e espera as que estão em voo terminarem antes de retornar.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		err := c.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		c.log.Error("sessão com o RabbitMQ encerrada, reconectando", "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}
	}
}

// session abre conexão e canal, consome e só retorna quando ctx é cancelado ou a conexão cai.
//
// A fila é declarada de forma passiva: quem declara a topologia é o definitions.json, assim os
// argumentos de dead-letter ficam num lugar só e o worker não os contradiz.
func (c *Consumer) session(ctx context.Context) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("conectar: %w", err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abrir canal: %w", err)
	}
	if err := channel.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	if _, err := channel.QueueDeclarePassive(c.queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("fila %s: %w", c.queue, err)
	}
	const consumerTag = "worker-go"
	deliveries, err := channel.Consume(c.queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consumir: %w", err)
	}
	c.log.Info("consumindo", "queue", c.queue, "prefetch", c.prefetch, "pool_size", c.poolSize)

	var pool sync.WaitGroup
	for range c.poolSize {
		pool.Add(1)
		go func() {
			defer pool.Done()
			for delivery := range deliveries {
				c.handle(delivery)
			}
		}()
	}

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	var sessionErr error
	select {
	case <-ctx.Done():
		// Cancelar o consumer fecha o canal de deliveries depois de entregar o que já veio,
		// e o pool termina o que está em voo antes de a conexão fechar.
		if err := channel.Cancel(consumerTag, false); err != nil {
			c.log.Warn("cancelar consumer", "error", err)
		}
	case amqpErr := <-closed:
		sessionErr = fmt.Errorf("conexão fechada: %v", amqpErr)
	}
	pool.Wait()
	return sessionErr
}

// handle processa uma entrega e aplica o destino decidido por Decide.
//
// O processamento usa um contexto sem cancelamento de propósito: no shutdown queremos terminar
// e gravar o job em voo, não abortá-lo no meio e gravar um `failed` por causa do encerramento.
func (c *Consumer) handle(delivery amqp.Delivery) {
	err := c.processor.Process(context.WithoutCancel(context.Background()), delivery.Body)
	disposition := Decide(err, delivery.Redelivered)
	switch disposition {
	case Ack:
		c.logIfFails(delivery.Ack(false), "ack")
	case RejectToDLQ:
		c.metrics.Processed.WithLabelValues("rejected").Inc()
		c.log.Warn("mensagem rejeitada para a DLQ", "error", err)
		c.logIfFails(delivery.Reject(false), "reject")
	case Requeue:
		c.log.Error("falha de infraestrutura, devolvendo para a fila", "error", err)
		c.logIfFails(delivery.Reject(true), "requeue")
	}
}

// logIfFails registra falha ao falar com o broker (a mensagem será reentregue pelo broker).
func (c *Consumer) logIfFails(err error, action string) {
	if err != nil {
		c.log.Error("falha ao confirmar com o broker", "action", action, "error", err)
	}
}
