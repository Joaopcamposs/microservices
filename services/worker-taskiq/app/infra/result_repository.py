"""Acesso ao Postgres: grava o resultado do job em `job_results`."""

import json

import asyncpg

from app.domain.models import JobResult

# `ON CONFLICT DO NOTHING` torna a gravação idempotente: a entrega é at-least-once, então o
# mesmo job pode chegar duas vezes e o segundo resultado é descartado sem erro. O `$n::jsonb`
# é necessário porque o asyncpg recebe JSON como texto.
_INSERT_RESULT = """
INSERT INTO job_results (job_id, worker, status, started_at, finished_at, result, error)
VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7)
ON CONFLICT (job_id, worker) DO NOTHING
"""


class ResultRepository:
    """Dono do pool de conexões e das escritas em `job_results`."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Recebe o pool já criado (quem cria também fecha, em `Services`)."""
        self._pool = pool

    async def save(self, result: JobResult) -> bool:
        """Grava o resultado; devolve `False` se já existia (entrega duplicada)."""
        payload = None if result.result is None else json.dumps(result.result)
        status = await self._pool.execute(
            _INSERT_RESULT,
            result.job_id,
            result.worker,
            result.status.value,
            result.started_at,
            result.finished_at,
            payload,
            result.error,
        )
        return status == "INSERT 0 1"
