"""Acesso ao Postgres: grava o resultado do job em `job_results` (psycopg síncrono)."""

import json

from psycopg_pool import ConnectionPool

from app.domain.models import JobResult

# `ON CONFLICT DO NOTHING` torna a gravação idempotente: a entrega é at-least-once, então o
# mesmo job pode chegar duas vezes e o segundo resultado é descartado sem erro. O `%s::jsonb`
# é necessário porque o JSON viaja como texto.
_INSERT_RESULT = """
INSERT INTO job_results (job_id, worker, status, started_at, finished_at, result, error)
VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s)
ON CONFLICT (job_id, worker) DO NOTHING
"""


class ResultRepository:
    """Escritas em `job_results` usando um pool criado (e fechado) por quem monta o processo."""

    def __init__(self, pool: ConnectionPool) -> None:
        """Recebe o pool já criado. No Celery há um pool por processo filho (fork-safe)."""
        self._pool = pool

    def save(self, result: JobResult) -> bool:
        """Grava o resultado; devolve `False` se já existia (entrega duplicada)."""
        payload = None if result.result is None else json.dumps(result.result)
        with self._pool.connection() as connection:
            cursor = connection.execute(
                _INSERT_RESULT,
                (
                    result.job_id,
                    result.worker,
                    result.status.value,
                    result.started_at,
                    result.finished_at,
                    payload,
                    result.error,
                ),
            )
            return cursor.rowcount == 1
