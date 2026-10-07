// Package rabbitmq implementa a publicação no broker com publisher confirms.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"microservices-lab/outbox-relay/internal/outbox"
)

const (
	// fanoutExchange entrega a mensagem às 4 filas (target=all).
	fanoutExchange = "jobs"
	// directExchange entrega a uma fila só, pela routing key igual ao nome da stack.
	directExchange = "jobs.direct"
	// returnsBuffer comporta os basic.return de um lote inteiro (ver config.maxBatchSize).
	returnsBuffer = 1024
)

// Publisher publica mensagens da outbox no RabbitMQ.
//
// Reconecta sozinho: depois de uma falha descarta a conexão e abre outra na próxima chamada.
// Não é seguro para uso concorrente; o relay o usa a partir de uma única goroutine.
type Publisher struct {
	url     string
	log     *slog.Logger
	conn    *amqp.Connection
	channel *amqp.Channel
	returns chan amqp.Return
}

// NewPublisher cria o publisher; a conexão é aberta de forma preguiçosa na primeira publicação,
// então o relay sobe mesmo com o broker fora do ar e passa a publicar quando ele voltar.
func NewPublisher(url string, log *slog.Logger) *Publisher {
	return &Publisher{url: url, log: log}
}

// Publish envia o lote e devolve os IDs confirmados pelo broker.
//
// As mensagens são enviadas todas antes de esperar os confirms (mais vazão que um confirm por
// mensagem). Uma mensagem só conta como publicada se o broker deu ack e não a devolveu como
// inroteável (mandatory). Em erro de conexão, a conexão é descartada e o erro é devolvido junto
// com o que já foi confirmado.
func (p *Publisher) Publish(ctx context.Context, messages []outbox.Message) ([]int64, error) {
	if err := p.ensureChannel(); err != nil {
		return nil, err
	}

	sent, sendErr := p.sendAll(ctx, messages)
	confirmedIDs, confirmErr := p.awaitConfirms(ctx, sent)

	returned := p.drainReturned()
	published := make([]int64, 0, len(confirmedIDs))
	var unroutable []string
	for _, message := range messages {
		if !contains(confirmedIDs, message.ID) {
			continue
		}
		if _, wasReturned := returned[message.JobID]; wasReturned {
			unroutable = append(unroutable, message.JobID)
			continue
		}
		published = append(published, message.ID)
	}

	err := errors.Join(sendErr, confirmErr)
	if len(unroutable) > 0 {
		err = errors.Join(err, fmt.Errorf("mensagens sem rota no broker: %v", unroutable))
	}
	if sendErr != nil || confirmErr != nil {
		p.reset()
	}
	return published, err
}

// pendingConfirm associa uma mensagem enviada à confirmação que o broker ainda vai dar.
type pendingConfirm struct {
	messageID    int64
	confirmation *amqp.DeferredConfirmation
}

// sendAll publica as mensagens em ordem e para na primeira falha de envio.
func (p *Publisher) sendAll(ctx context.Context, messages []outbox.Message) ([]pendingConfirm, error) {
	sent := make([]pendingConfirm, 0, len(messages))
	for _, message := range messages {
		exchange, routingKey := routeFor(message)
		confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
			ctx,
			exchange,
			routingKey,
			true,  // mandatory: o broker devolve a mensagem se nenhuma fila a recebe.
			false, // immediate: obsoleto no RabbitMQ.
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    message.JobID,
				Timestamp:    message.CreatedAt,
				Body:         message.Envelope,
			},
		)
		if err != nil {
			return sent, fmt.Errorf("publicar job %s: %w", message.JobID, err)
		}
		sent = append(sent, pendingConfirm{messageID: message.ID, confirmation: confirmation})
	}
	return sent, nil
}

// awaitConfirms espera o ack de cada mensagem enviada e devolve os IDs com ack positivo.
func (p *Publisher) awaitConfirms(ctx context.Context, sent []pendingConfirm) ([]int64, error) {
	var confirmed []int64
	var firstErr error
	for _, item := range sent {
		acked, err := item.confirmation.WaitContext(ctx)
		switch {
		case err != nil:
			firstErr = errors.Join(firstErr, fmt.Errorf("aguardar confirm: %w", err))
		case !acked:
			firstErr = errors.Join(firstErr, fmt.Errorf("broker recusou (nack) a mensagem %d", item.messageID))
		default:
			confirmed = append(confirmed, item.messageID)
		}
	}
	return confirmed, firstErr
}

// drainReturned esvazia, sem bloquear, as mensagens que o broker devolveu como inroteáveis.
//
// O AMQP envia o basic.return antes do ack da mesma mensagem, então depois de aguardar todos
// os confirms os retornos do lote já estão no canal.
func (p *Publisher) drainReturned() map[string]struct{} {
	returned := make(map[string]struct{})
	for {
		select {
		case message := <-p.returns:
			returned[message.MessageId] = struct{}{}
		default:
			return returned
		}
	}
}

// ensureChannel abre conexão e canal em modo de confirmação se ainda não existirem.
func (p *Publisher) ensureChannel() error {
	if p.channel != nil && !p.channel.IsClosed() {
		return nil
	}
	p.reset()

	conn, err := amqp.DialConfig(p.url, amqp.Config{Dial: amqp.DefaultDial(10 * time.Second)})
	if err != nil {
		return fmt.Errorf("conectar ao RabbitMQ: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("abrir canal: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ativar publisher confirms: %w", err)
	}

	p.conn = conn
	p.channel = channel
	p.returns = channel.NotifyReturn(make(chan amqp.Return, returnsBuffer))
	p.log.Info("conectado ao RabbitMQ")
	return nil
}

// reset descarta a conexão atual para a próxima chamada reconectar.
func (p *Publisher) reset() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn, p.channel, p.returns = nil, nil, nil
}

// Close encerra a conexão com o broker; chamado no shutdown.
func (p *Publisher) Close() error {
	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn, p.channel, p.returns = nil, nil, nil
	return err
}

// routeFor escolhe o exchange: routing key vazia vai ao fanout, preenchida ao direct.
func routeFor(message outbox.Message) (exchange, routingKey string) {
	if message.RoutingKey == "" {
		return fanoutExchange, ""
	}
	return directExchange, message.RoutingKey
}

// contains informa se id está em ids.
func contains(ids []int64, id int64) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}
