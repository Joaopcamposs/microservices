"""Validação do payload de um job contra o JSON Schema do seu tipo."""

import json
from pathlib import Path
from typing import Self

from jsonschema import Draft202012Validator

from app.core.value_objects import JobType, JsonObject


class UnsupportedJobTypeError(Exception):
    """O tipo existe no contrato, mas ainda não tem schema de payload implementado."""


class InvalidPayloadError(Exception):
    """Payload viola o JSON Schema do tipo de job."""


class PayloadValidator:
    """Valida o payload contra `contracts/jobs/<type>.schema.json`.

    A fonte da verdade é o schema compartilhado, não código Python: o gateway Go lê os mesmos
    arquivos, o que garante que os dois aceitem e rejeitem exatamente os mesmos pedidos.
    """

    def __init__(self, validators: dict[JobType, Draft202012Validator]) -> None:
        """Recebe validadores já compilados, um por tipo de job com schema."""
        self._validators = validators

    @classmethod
    def from_directory(cls, directory: Path) -> Self:
        """Carrega os schemas existentes na pasta e ignora os tipos que ainda não têm arquivo.

        Cada schema é conferido com `check_schema` no boot: um schema quebrado derruba a
        subida do serviço em vez de falhar só na primeira requisição.
        """
        validators: dict[JobType, Draft202012Validator] = {}
        for job_type in JobType:
            schema_path = directory / f"{job_type.value}.schema.json"
            if schema_path.is_file():
                schema = json.loads(schema_path.read_text())
                Draft202012Validator.check_schema(schema)
                validators[job_type] = Draft202012Validator(schema)
        return cls(validators)

    def validate(self, job_type: JobType, payload: JsonObject) -> None:
        """Levanta `UnsupportedJobTypeError` se o tipo não tem schema e `InvalidPayloadError`
        com todas as violações juntas (ordenadas pelo caminho) se o payload não confere.
        """
        validator = self._validators.get(job_type)
        if validator is None:
            raise UnsupportedJobTypeError(f"tipo de job sem schema: {job_type.value}")
        errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.path))
        if errors:
            raise InvalidPayloadError("; ".join(error.message for error in errors))
