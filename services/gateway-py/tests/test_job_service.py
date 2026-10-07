"""Testes do `JobService` com um armazenamento falso, sem banco."""

import json
from pathlib import Path
from uuid import UUID

from jsonschema import Draft202012Validator

from app.core.value_objects import JobType, Origin, OutboxState, Target
from app.domain.models import Envelope, JobView, OutboxEntry
from app.domain.payload_validator import PayloadValidator
from app.services.job_service import JobService

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"


class FakeStore:
    """Implementa `JobStore` em memória e guarda o que o serviço tentou persistir."""

    def __init__(self) -> None:
        """Começa sem nenhum job enfileirado."""
        self.enqueued: list[tuple[Envelope, Target]] = []

    async def enqueue(self, envelope: Envelope, target: Target) -> None:
        """Registra o envelope em vez de gravar no banco."""
        self.enqueued.append((envelope, target))

    async def find(self, job_id: UUID) -> JobView | None:
        """Não usado nestes testes."""
        return None

    async def list_recent(self, limit: int) -> list[JobView]:
        """Não usado nestes testes."""
        return []

    async def list_outbox(self, state: OutboxState, limit: int) -> list[OutboxEntry]:
        """Não usado nestes testes."""
        return []

    async def ping(self) -> None:
        """Não usado nestes testes."""


async def test_submit_builds_envelope_that_satisfies_contract() -> None:
    """O envelope montado pelo serviço valida contra `envelope.schema.json`.

    Protege o contrato único: se o gateway passar a gerar um campo fora do schema, os workers
    em outras linguagens quebrariam, e este teste falha antes.
    """
    store = FakeStore()
    service = JobService(
        store, PayloadValidator.from_directory(CONTRACTS / "jobs"), Origin.GATEWAY_PY
    )

    job_id = await service.submit(JobType.IO_SLEEP, Target.GO, {"ms": 10})

    envelope, target = store.enqueued[0]
    assert envelope.job_id == job_id
    assert target is Target.GO
    schema = json.loads((CONTRACTS / "envelope.schema.json").read_text())
    Draft202012Validator(schema).validate(json.loads(envelope.to_json()))
