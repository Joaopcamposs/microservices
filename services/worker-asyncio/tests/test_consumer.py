"""Testes da decisão de ack/reject do consumidor, com mensagem e processor falsos."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import CollectorRegistry

from app.domain.contracts import InvalidMessageError
from app.infra.consumer import QueueConsumer
from app.services.metrics import WorkerMetrics


def make_message(redelivered: bool = False) -> MagicMock:
    """Mensagem falsa com ack/reject assíncronos observáveis."""
    message = MagicMock(body=b"{}", redelivered=redelivered)
    message.ack = AsyncMock()
    message.reject = AsyncMock()
    return message


def make_consumer(error: Exception | None) -> QueueConsumer:
    """Consumidor cujo processor falha com `error` (ou funciona, se `None`)."""
    processor = MagicMock()
    processor.process = AsyncMock(side_effect=error)
    return QueueConsumer("amqp://x", "q", 1, processor, WorkerMetrics(CollectorRegistry()))


async def test_success_acks() -> None:
    """Sucesso confirma a mensagem (e só depois do processamento)."""
    message = make_message()
    await make_consumer(None)._on_message(message)
    message.ack.assert_awaited_once()


async def test_invalid_message_goes_to_dlq() -> None:
    """Mensagem inválida é rejeitada sem requeue, para a DLX/DLQ."""
    message = make_message()
    await make_consumer(InvalidMessageError("x"))._on_message(message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


@pytest.mark.parametrize(("redelivered", "requeue"), [(False, True), (True, False)])
async def test_infra_failure_requeues_only_once(redelivered: bool, requeue: bool) -> None:
    """Falha de infra recebe uma segunda chance; se já foi reentregue, vai para a DLQ."""
    message = make_message(redelivered)
    await make_consumer(RuntimeError("db fora"))._on_message(message)
    message.reject.assert_awaited_once_with(requeue=requeue)
