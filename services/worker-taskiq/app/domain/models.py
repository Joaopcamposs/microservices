"""Modelos de domínio do worker: o job recebido e o resultado a gravar."""

from dataclasses import dataclass
from datetime import datetime

from app.core.value_objects import JobType, JsonObject, ResultStatus


@dataclass(frozen=True, slots=True)
class Job:
    """Envelope já validado, só com os campos que o worker usa.

    `traceparent` e `attempt` seguem junto para os logs; a propagação completa do trace
    entra na fase de observabilidade.
    """

    job_id: str
    type: JobType
    payload: JsonObject
    traceparent: str
    attempt: int


@dataclass(frozen=True, slots=True)
class JobResult:
    """Linha de `job_results`: uma por `(job_id, worker)`, gravada uma única vez."""

    job_id: str
    worker: str
    status: ResultStatus
    started_at: datetime
    finished_at: datetime
    result: JsonObject | None
    error: str | None
