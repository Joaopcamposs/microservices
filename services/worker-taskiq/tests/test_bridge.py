"""Testes da decisão de ack/reject da bridge, com mensagem e dispatcher falsos."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import CollectorRegistry

from app.core.value_objects import JobType
from app.domain.contracts import ContractValidator
from app.infra.bridge import Bridge
from app.services.bridge_metrics import BridgeMetrics
from tests.test_contracts import CONTRACTS, make_body


def make_message(body: bytes, redelivered: bool = False) -> MagicMock:
    """Mensagem falsa com ack/reject assíncronos observáveis."""
    message = MagicMock(body=body, redelivered=redelivered)
    message.ack = AsyncMock()
    message.reject = AsyncMock()
    return message


def make_bridge(dispatcher: MagicMock) -> Bridge:
    """Bridge com o contrato real e `io.sleep` como único tipo suportado."""
    return Bridge(
        "amqp://x",
        "q",
        1,
        ContractValidator.from_directory(CONTRACTS),
        frozenset({JobType.IO_SLEEP}),
        dispatcher,
        BridgeMetrics(CollectorRegistry()),
    )


async def test_valid_message_is_dispatched_then_acked() -> None:
    """Mensagem válida é entregue ao framework e só então confirmada."""
    dispatcher = MagicMock(submit=AsyncMock())
    message = make_message(make_body())
    await make_bridge(dispatcher)._on_message(message)
    dispatcher.submit.assert_awaited_once_with(message.body)
    message.ack.assert_awaited_once()


@pytest.mark.parametrize(
    "body",
    [b"lixo", make_body(payload={"ms": -1}), make_body(type="io.fetch_urls", payload={})],
)
async def test_invalid_message_goes_to_dlq_without_dispatch(body: bytes) -> None:
    """Inválida (ou sem handler) vai para a DLQ e não consome uma task do framework."""
    dispatcher = MagicMock(submit=AsyncMock())
    message = make_message(body)
    await make_bridge(dispatcher)._on_message(message)
    dispatcher.submit.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


@pytest.mark.parametrize(("redelivered", "requeue"), [(False, True), (True, False)])
async def test_dispatch_failure_requeues_only_once(redelivered: bool, requeue: bool) -> None:
    """Broker do framework fora recebe uma segunda chance; reentregue, vai para a DLQ."""
    dispatcher = MagicMock(submit=AsyncMock())
    dispatcher.submit.side_effect = ConnectionError("broker fora")
    message = make_message(make_body(), redelivered)
    await make_bridge(dispatcher)._on_message(message)
    message.reject.assert_awaited_once_with(requeue=requeue)
    message.ack.assert_not_awaited()
