"""Validação de mensagens contra os JSON Schemas compartilhados em `contracts/`."""

import json
from pathlib import Path
from typing import Self

from jsonschema import Draft202012Validator

from app.core.value_objects import JobType
from app.domain.models import Job


class InvalidMessageError(Exception):
    """Mensagem que nunca vai passar: JSON quebrado, envelope fora do contrato, payload inválido
    ou tipo sem handler. Reentregar não adianta, então ela segue para a DLQ.
    """


class ContractValidator:
    """Converte o corpo bruto da mensagem em `Job`, aplicando envelope e payload schema.

    Fonte da verdade são os arquivos de `contracts/`, os mesmos que os gateways usam: o worker
    rejeita exatamente o que o contrato rejeita, sem regras duplicadas em Python.
    """

    def __init__(
        self,
        envelope_validator: Draft202012Validator,
        payload_validators: dict[JobType, Draft202012Validator],
    ) -> None:
        """Recebe validadores já compilados (envelope e um por tipo de job com schema)."""
        self._envelope = envelope_validator
        self._payloads = payload_validators

    @classmethod
    def from_directory(cls, contracts_dir: Path) -> Self:
        """Carrega `envelope.schema.json` e os `jobs/<type>.schema.json` existentes.

        Cada schema passa por `check_schema` no boot: schema quebrado derruba a subida em vez
        de rejeitar mensagens boas em runtime.
        """
        envelope_schema = json.loads((contracts_dir / "envelope.schema.json").read_text())
        Draft202012Validator.check_schema(envelope_schema)
        payloads: dict[JobType, Draft202012Validator] = {}
        for job_type in JobType:
            path = contracts_dir / "jobs" / f"{job_type.value}.schema.json"
            if path.is_file():
                schema = json.loads(path.read_text())
                Draft202012Validator.check_schema(schema)
                payloads[job_type] = Draft202012Validator(schema)
        return cls(Draft202012Validator(envelope_schema), payloads)

    def parse(self, body: bytes) -> Job:
        """Valida o corpo e devolve o `Job`; levanta `InvalidMessageError` com o motivo."""
        try:
            raw = json.loads(body)
        except ValueError as error:
            raise InvalidMessageError(f"corpo não é JSON: {error}") from error
        errors = sorted(self._envelope.iter_errors(raw), key=lambda e: list(e.path))
        if errors:
            raise InvalidMessageError("envelope inválido: " + "; ".join(e.message for e in errors))
        job_type = JobType(raw["type"])
        payload_validator = self._payloads.get(job_type)
        if payload_validator is None:
            raise InvalidMessageError(f"tipo sem schema de payload: {job_type.value}")
        payload_errors = sorted(
            payload_validator.iter_errors(raw["payload"]), key=lambda e: list(e.path)
        )
        if payload_errors:
            raise InvalidMessageError(
                "payload inválido: " + "; ".join(e.message for e in payload_errors)
            )
        return Job(
            job_id=raw["job_id"],
            type=job_type,
            payload=raw["payload"],
            traceparent=raw["traceparent"],
            attempt=raw["attempt"],
        )
