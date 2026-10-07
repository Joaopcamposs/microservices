"""Modelos Pydantic da borda HTTP.

Separados do domínio de propósito: definem o que a API expõe (e o que o Swagger mostra),
podendo evoluir sem mexer nas `dataclass` internas.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.core.value_objects import JobStatus, JobType, JsonObject, Origin, ResultStatus, Target


class OutboxEntryOut(BaseModel):
    """Linha da outbox exposta em `GET /outbox`."""

    id: int
    job_id: UUID
    routing_key: str
    created_at: datetime
    published_at: datetime | None


class JobAccepted(BaseModel):
    """Resposta `202` do `POST /jobs`: só o identificador para consultar depois."""

    job_id: UUID


class WorkerResultOut(BaseModel):
    """Resultado de um worker dentro de `JobOut`."""

    worker: str
    status: ResultStatus
    started_at: datetime
    finished_at: datetime
    result: JsonObject | None
    error: str | None


class JobOut(BaseModel):
    """Job com status agregado e resultados, devolvido por `GET /jobs` e `GET /jobs/{id}`."""

    job_id: UUID
    type: JobType
    target: Target
    origin: Origin
    created_at: datetime
    status: JobStatus
    results: list[WorkerResultOut]
