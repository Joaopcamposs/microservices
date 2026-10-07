"""Teste de integração do repositório contra o Postgres do compose (pulado se estiver fora)."""

import uuid
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from app.core.settings import Settings
from app.core.value_objects import ResultStatus
from app.domain.models import JobResult
from app.infra.result_repository import ResultRepository


def test_save_is_idempotent_per_job_and_worker() -> None:
    """Gravar o mesmo `(job_id, worker)` duas vezes deixa uma linha e devolve False na segunda."""
    url = Settings().database_url
    try:
        connection = psycopg.connect(url, connect_timeout=3)
    except psycopg.OperationalError as error:
        pytest.skip(f"Postgres indisponível (rode `make up`): {error}")
    job_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    result = JobResult(job_id, "celery", ResultStatus.SUCCEEDED, now, now, {"slept_ms": 1}, None)
    pool = ConnectionPool(url, min_size=1, max_size=1, open=True)
    try:
        connection.execute(
            "INSERT INTO jobs (job_id, type, target, origin) "
            "VALUES (%s::uuid, 'io.sleep', 'celery', 'gateway-py')",
            (job_id,),
        )
        connection.commit()
        repository = ResultRepository(pool)
        assert repository.save(result) is True
        assert repository.save(result) is False
        row = connection.execute(
            "SELECT count(*) FROM job_results WHERE job_id = %s::uuid", (job_id,)
        ).fetchone()
        assert row == (1,)
    finally:
        connection.execute("DELETE FROM job_results WHERE job_id = %s::uuid", (job_id,))
        connection.execute("DELETE FROM jobs WHERE job_id = %s::uuid", (job_id,))
        connection.commit()
        pool.close()
        connection.close()
