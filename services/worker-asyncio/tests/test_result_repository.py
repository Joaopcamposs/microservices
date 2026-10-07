"""Teste de integração do repositório contra o Postgres do compose (pulado se estiver fora)."""

import uuid
from datetime import UTC, datetime

import asyncpg
import pytest

from app.core.settings import Settings
from app.core.value_objects import ResultStatus
from app.domain.models import JobResult
from app.infra.result_repository import ResultRepository


async def test_save_is_idempotent_per_job_and_worker() -> None:
    """Gravar o mesmo `(job_id, worker)` duas vezes deixa uma linha e devolve False na segunda."""
    try:
        pool = await asyncpg.create_pool(Settings().database_url, timeout=3)
    except (OSError, asyncpg.PostgresError) as error:
        pytest.skip(f"Postgres indisponível (rode `make up`): {error}")
    job_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    result = JobResult(job_id, "asyncio", ResultStatus.SUCCEEDED, now, now, {"slept_ms": 1}, None)
    try:
        await pool.execute(
            "INSERT INTO jobs (job_id, type, target, origin) "
            "VALUES ($1::uuid, 'io.sleep', 'asyncio', 'gateway-py')",
            job_id,
        )
        repository = ResultRepository(pool)
        assert await repository.save(result) is True
        assert await repository.save(result) is False
        count = await pool.fetchval(
            "SELECT count(*) FROM job_results WHERE job_id = $1::uuid", job_id
        )
        assert count == 1
    finally:
        await pool.execute("DELETE FROM job_results WHERE job_id = $1::uuid", job_id)
        await pool.execute("DELETE FROM jobs WHERE job_id = $1::uuid", job_id)
        await pool.close()
