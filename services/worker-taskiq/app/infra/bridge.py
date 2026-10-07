"""Bridge: lê o envelope neutro de `jobs.<stack>` e o entrega ao framework."""

import asyncio
import logging
from typing import Protocol

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.core.value_objects import JobType
from app.domain.contracts import ContractValidator, InvalidMessageError
from app.services.bridge_metrics import BridgeMetrics

logger = logging.getLogger(__name__)


class Dispatcher(Protocol):
    """O que a bridge precisa do framework: aceitar o envelope ou levantar erro."""

    async def submit(self, body: bytes) -> None:
        """Entrega o corpo da mensagem ao framework (aguarda a confirmação do broker)."""


class Bridge:
    """Consome `jobs.<stack>` com ack manual e repassa ao framework (Celery ou TaskIQ).

    Regra de ack da bridge: confirma a mensagem original **depois** de o broker aceitar a task do
    framework. É um ack de entrega, não de resultado: o resultado é gravado depois, pelo worker
    do framework, de forma idempotente. A janela de perda é a mesma de qualquer fila durável
    com confirm. Mensagem inválida vai para a DLQ aqui, antes de gastar uma task.
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        prefetch: int,
        validator: ContractValidator,
        supported_types: frozenset[JobType],
        dispatcher: Dispatcher,
        metrics: BridgeMetrics,
    ) -> None:
        """Guarda a configuração; a conexão só abre em `run`."""
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._prefetch = prefetch
        self._validator = validator
        self._supported_types = supported_types
        self._dispatcher = dispatcher
        self._metrics = metrics

    async def run(self, stop: asyncio.Event) -> None:
        """Conecta (com reconexão automática), consome até `stop` ser sinalizado e fecha.

        A fila é passiva: quem declara a topologia é o `definitions.json` do RabbitMQ.
        """
        connection = await aio_pika.connect_robust(self._amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self._prefetch)
            queue = await channel.declare_queue(self._queue_name, passive=True)
            await queue.consume(self._on_message)
            logger.info("bridge consumindo %s (prefetch=%d)", self._queue_name, self._prefetch)
            await stop.wait()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Valida, entrega ao framework e traduz o resultado em ack, reject ou requeue."""
        try:
            self._check(message.body)
            await self._dispatcher.submit(message.body)
        except InvalidMessageError as error:
            logger.warning("mensagem rejeitada para a DLQ: %s", error)
            self._metrics.forwarded.labels(outcome="rejected").inc()
            await message.reject(requeue=False)
        except Exception:
            logger.exception("falha ao entregar ao framework")
            # Segunda chance para uma queda curta do broker; na segunda falha vai para a DLQ,
            # evitando o requeue infinito.
            self._metrics.forwarded.labels(outcome="requeued").inc()
            await message.reject(requeue=not message.redelivered)
        else:
            self._metrics.forwarded.labels(outcome="forwarded").inc()
            await message.ack()

    def _check(self, body: bytes) -> None:
        """Rejeita o que o framework nunca conseguirá processar: contrato ou tipo sem handler."""
        job = self._validator.parse(body)
        if job.type not in self._supported_types:
            raise InvalidMessageError(f"sem handler para o tipo {job.type.value}")
