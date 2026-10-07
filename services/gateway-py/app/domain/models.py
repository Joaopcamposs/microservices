"""Modelos de domínio imutáveis.

São `dataclass` puras, sem ORM nem Pydantic: o domínio não depende de como os dados chegam
(HTTP) nem de onde ficam (Postgres). A conversão acontece nas bordas (`api/` e `infra/`).
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from app.core.value_objects import (
    WORKERS_PER_FANOUT,
    JobStatus,
    JobType,
    JsonObject,
    Origin,
    ResultStatus,
    Target,
)


@dataclass(frozen=True, slots=True)
class Envelope:
    """Mensagem neutra definida em `contracts/envelope.schema.json`.

    É o contrato único entre gateways e workers: nenhum framework (Celery, TaskIQ) dita o formato.
    """

    job_id: UUID
    type: JobType
    payload: JsonObject
    created_at: datetime
    traceparent: str
    attempt: int
    origin: Origin

    def to_json(self) -> str:
        """Serializa no formato do contrato.

        `created_at` sai em UTC com sufixo `Z`, como o schema espera (`date-time`). Este JSON é
        gravado na outbox e publicado pelo relay sem nenhuma reinterpretação.
        """
        created_at = self.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return json.dumps(
            {
                "job_id": str(self.job_id),
                "type": self.type.value,
                "payload": self.payload,
                "created_at": created_at,
                "traceparent": self.traceparent,
                "attempt": self.attempt,
                "origin": self.origin.value,
            }
        )


@dataclass(frozen=True, slots=True)
class JobRecord:
    """Linha da tabela `jobs`: o pedido original, sem os resultados."""

    job_id: UUID
    type: JobType
    target: Target
    origin: Origin
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """Linha da tabela `job_results`: o que um worker específico produziu para o job."""

    worker: str
    status: ResultStatus
    started_at: datetime
    finished_at: datetime
    result: JsonObject | None
    error: str | None


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """Linha da tabela `outbox`, sem o envelope (que pode ser grande). Usada para inspeção."""

    id: int
    job_id: UUID
    routing_key: str
    created_at: datetime
    published_at: datetime | None


@dataclass(frozen=True, slots=True)
class JobView:
    """Job junto com os resultados dos workers: o que a consulta da API devolve."""

    record: JobRecord
    results: list[WorkerResult]

    @property
    def expected_workers(self) -> int:
        """Quantos resultados o job espera: 4 no fanout, 1 quando há um target específico."""
        return WORKERS_PER_FANOUT if self.record.target is Target.ALL else 1

    @property
    def status(self) -> JobStatus:
        """Deriva o estado dos resultados já gravados; não há coluna de status no banco.

        Calcular em vez de persistir evita um segundo write por worker e o risco de o campo
        divergir da tabela `job_results`, que é a fonte da verdade.
        """
        if not self.results:
            return JobStatus.PENDING
        if len(self.results) >= self.expected_workers:
            return JobStatus.COMPLETED
        return JobStatus.RUNNING
