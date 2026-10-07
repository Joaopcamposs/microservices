"""Testes do `PayloadValidator`: o contrato de payload é aplicado como o schema define."""

from pathlib import Path

import pytest

from app.core.value_objects import JobType
from app.domain.payload_validator import (
    InvalidPayloadError,
    PayloadValidator,
    UnsupportedJobTypeError,
)

CONTRACTS_JOBS = Path(__file__).resolve().parents[3] / "contracts" / "jobs"


@pytest.fixture
def validator() -> PayloadValidator:
    """Carrega os schemas reais de `contracts/jobs`, os mesmos que o gateway usa."""
    return PayloadValidator.from_directory(CONTRACTS_JOBS)


def test_accepts_valid_io_sleep_payload(validator: PayloadValidator) -> None:
    """Um payload que segue o schema não levanta erro."""
    validator.validate(JobType.IO_SLEEP, {"ms": 100})


@pytest.mark.parametrize("payload", [{}, {"ms": -1}, {"ms": "100"}, {"ms": 1, "extra": 1}])
def test_rejects_invalid_io_sleep_payload(validator: PayloadValidator, payload: dict) -> None:
    """Falta de `ms`, valor negativo, tipo errado e campo extra são rejeitados."""
    with pytest.raises(InvalidPayloadError):
        validator.validate(JobType.IO_SLEEP, payload)


def test_rejects_type_without_schema(validator: PayloadValidator) -> None:
    """Tipo previsto no contrato mas sem schema é recusado, não aceito às cegas."""
    with pytest.raises(UnsupportedJobTypeError):
        validator.validate(JobType.CPU_PBKDF2, {})
