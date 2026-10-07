"""Consumidor RabbitMQ: liga a fila ao `JobProcessor` e decide ack ou reject."""

import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.domain.contracts import InvalidMessageError
from app.services.job_processor import JobProcessor
from app.services.metrics import WorkerMetrics

logger = logging.getLogger(__name__)


class QueueConsumer:
    """Consome uma fila com ack manual.

    Regra do projeto: o ack só acontece **depois** de o resultado estar gravado. Se o worker
    cair no meio, o broker reentrega (at-least-once) e a gravação idempotente absorve a duplicata.
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        prefetch: int,
        processor: JobProcessor,
        metrics: WorkerMetrics,
    ) -> None:
        """Guarda a configuração; a conexão só abre em `run`."""
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._prefetch = prefetch
        self._processor = processor
        self._metrics = metrics

    async def run(self, stop: asyncio.Event) -> None:
        """Conecta (com reconexão automática), consome até `stop` ser sinalizado e fecha.

        A fila é passiva: quem declara a topologia é o `definitions.json` do RabbitMQ, assim
        os argumentos de dead-letter ficam num lugar só e o worker não os contradiz.
        """
        connection = await aio_pika.connect_robust(self._amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self._prefetch)
            queue = await channel.declare_queue(self._queue_name, passive=True)
            await queue.consume(self._on_message)
            logger.info("consumindo %s (prefetch=%d)", self._queue_name, self._prefetch)
            await stop.wait()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Processa uma mensagem e traduz o resultado em ack, reject para DLQ ou requeue."""
        try:
            await self._processor.process(message.body)
        except InvalidMessageError as error:
            logger.warning("mensagem rejeitada para a DLQ: %s", error)
            self._metrics.processed.labels(status="rejected").inc()
            await message.reject(requeue=False)
        except Exception:
            logger.exception("falha de infraestrutura ao processar a mensagem")
            # Uma segunda chance cobre uma queda curta do banco; na segunda falha vai para a
            # DLQ, evitando o requeue infinito que travaria a fila.
            await message.reject(requeue=not message.redelivered)
        else:
            await message.ack()
