"""Testes da validação de mensagens contra os schemas reais de `contracts/`."""

import json
from pathlib import Path

import pytest

from app.domain.contracts import ContractValidator, InvalidMessageError

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"


def make_body(**overrides: object) -> bytes:
    """Monta um envelope válido de `io.sleep`, permitindo sobrescrever campos."""
    envelope = {
        "job_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
        "type": "io.sleep",
        "payload": {"ms": 10},
        "created_at": "2026-10-07T12:00:00Z",
        "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        "attempt": 0,
        "origin": "gateway-py",
    }
    return json.dumps({**envelope, **overrides}).encode()


@pytest.fixture
def validator() -> ContractValidator:
    """Validador carregado dos arquivos reais, como no boot do worker."""
    return ContractValidator.from_directory(CONTRACTS)


def test_valid_message_becomes_job(validator: ContractValidator) -> None:
    """Envelope e payload válidos viram um `Job` com os campos do contrato."""
    job = validator.parse(make_body())
    assert job.job_id == "7c9e6679-7425-40de-944b-e07fc1f90ae7"
    assert job.payload == {"ms": 10}


@pytest.mark.parametrize(
    "body",
    [
        b"{nao e json",
        make_body(payload={"ms": -1}),
        make_body(payload={}),
        make_body(type="io.fetch_urls", payload={}),  # no contrato, mas sem schema de payload
        make_body(origin="outro"),
    ],
)
def test_unprocessable_message_is_invalid(validator: ContractValidator, body: bytes) -> None:
    """Mensagem que nunca vai passar levanta `InvalidMessageError` (segue para a DLQ)."""
    with pytest.raises(InvalidMessageError):
        validator.parse(body)
