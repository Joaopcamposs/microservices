"""Repositório do Postgres: única parte do gateway que conhece as tabelas.

Usa SQL explícito (`text`) em vez de ORM: o schema é pequeno e compartilhado com outros
serviços, e o SQL deixa visível o que cada operação faz no banco.
"""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.value_objects import (
    JobType,
    JsonObject,
    Origin,
    OutboxState,
    ResultStatus,
    Target,
)
from app.domain.models import Envelope, JobRecord, JobView, OutboxEntry, WorkerResult

_INSERT_JOB = text(
    "INSERT INTO jobs (job_id, type, target, origin, created_at) "
    "VALUES (:job_id, :type, :target, :origin, :created_at)"
)
# `CAST(... AS jsonb)`: o envelope chega como string JSON já pronta e é guardado sem alteração.
_INSERT_OUTBOX = text(
    "INSERT INTO outbox (job_id, routing_key, envelope) "
    "VALUES (:job_id, :routing_key, CAST(:envelope AS jsonb))"
)
_SELECT_JOB = text(
    "SELECT job_id, type, target, origin, created_at FROM jobs WHERE job_id = :job_id"
)
_SELECT_RESULTS = text(
    "SELECT worker, status, started_at, finished_at, result, error "
    "FROM job_results WHERE job_id = :job_id ORDER BY worker"
)
_SELECT_RECENT_JOBS = text(
    "SELECT job_id, type, target, origin, created_at FROM jobs "
    "ORDER BY created_at DESC LIMIT :limit"
)
# Uma query para os resultados de todos os jobs da listagem, evitando N+1 consultas.
_SELECT_RESULTS_OF_MANY = text(
    "SELECT job_id, worker, status, started_at, finished_at, result, error "
    "FROM job_results WHERE job_id = ANY(:job_ids) ORDER BY worker"
)
_SELECT_OUTBOX = text(
    "SELECT id, job_id, routing_key, created_at, published_at FROM outbox "
    "WHERE (:state = 'all') "
    "OR (:state = 'pending' AND published_at IS NULL) "
    "OR (:state = 'published' AND published_at IS NOT NULL) "
    "ORDER BY id DESC LIMIT :limit"
)


class JobRepository:
    """Acesso ao Postgres. O gateway nunca fala com o RabbitMQ: publicar é papel do relay."""

    def __init__(self, engine: AsyncEngine) -> None:
        """Recebe o engine pronto; quem o cria (e fecha) é `Services`."""
        self._engine = engine

    async def enqueue(self, envelope: Envelope, target: Target) -> None:
        """Grava `jobs` e `outbox` na mesma transação: ou as duas linhas existem, ou nenhuma.

        É o coração do padrão outbox: sem broker na transação, não há janela em que o job
        existe no banco mas a mensagem se perdeu (ou o contrário). `routing_key` vazia vai
        para o exchange fanout; preenchida, para o direct.
        """
        routing_key = "" if target is Target.ALL else target.value
        async with self._engine.begin() as conn:
            await conn.execute(
                _INSERT_JOB,
                {
                    "job_id": envelope.job_id,
                    "type": envelope.type.value,
                    "target": target.value,
                    "origin": envelope.origin.value,
                    "created_at": envelope.created_at,
                },
            )
            await conn.execute(
                _INSERT_OUTBOX,
                {
                    "job_id": envelope.job_id,
                    "routing_key": routing_key,
                    "envelope": envelope.to_json(),
                },
            )

    async def find(self, job_id: UUID) -> JobView | None:
        """Busca um job com os resultados dos workers; devolve `None` se o `job_id` não existe."""
        async with self._engine.connect() as conn:
            job_row = (await conn.execute(_SELECT_JOB, {"job_id": job_id})).one_or_none()
            if job_row is None:
                return None
            result_rows = (await conn.execute(_SELECT_RESULTS, {"job_id": job_id})).all()

        return _to_job_view(job_row, [_to_worker_result(row) for row in result_rows])

    async def list_recent(self, limit: int) -> list[JobView]:
        """Lista os jobs mais novos primeiro, cada um com seus resultados."""
        async with self._engine.connect() as conn:
            job_rows = (await conn.execute(_SELECT_RECENT_JOBS, {"limit": limit})).all()
            job_ids = [row.job_id for row in job_rows]
            result_rows = (await conn.execute(_SELECT_RESULTS_OF_MANY, {"job_ids": job_ids})).all()

        results_by_job: dict[UUID, list[WorkerResult]] = {job_id: [] for job_id in job_ids}
        for row in result_rows:
            results_by_job[row.job_id].append(_to_worker_result(row))
        return [_to_job_view(row, results_by_job[row.job_id]) for row in job_rows]

    async def list_outbox(self, state: OutboxState, limit: int) -> list[OutboxEntry]:
        """Lista linhas da outbox (mais novas primeiro) filtradas por estado.

        Serve para inspecionar se o relay está esvaziando a fila; não devolve o envelope.
        """
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(_SELECT_OUTBOX, {"state": state.value, "limit": limit})
            ).all()
        return [
            OutboxEntry(
                id=row.id,
                job_id=row.job_id,
                routing_key=row.routing_key,
                created_at=row.created_at,
                published_at=row.published_at,
            )
            for row in rows
        ]

    async def ping(self) -> None:
        """Executa `SELECT 1`; levanta exceção se o banco não responde. Usado pelo `/healthz`."""
        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))


def _to_job_view(row: Row[Any], results: list[WorkerResult]) -> JobView:
    """Converte a linha de `jobs` (e os resultados já convertidos) no modelo de domínio."""
    record = JobRecord(
        job_id=row.job_id,
        type=JobType(row.type),
        target=Target(row.target),
        origin=Origin(row.origin),
        created_at=row.created_at,
    )
    return JobView(record=record, results=results)


def _to_worker_result(row: Row[Any]) -> WorkerResult:
    """Converte uma linha de `job_results` no modelo de domínio."""
    return WorkerResult(
        worker=row.worker,
        status=ResultStatus(row.status),
        started_at=row.started_at,
        finished_at=row.finished_at,
        result=_parse_json_object(row.result),
        error=row.error,
    )


def _parse_json_object(raw: str | JsonObject | None) -> JsonObject | None:
    """Normaliza uma coluna JSONB: o asyncpg a devolve como `str` em queries textuais."""
    if isinstance(raw, str):
        return json.loads(raw)
    return raw
