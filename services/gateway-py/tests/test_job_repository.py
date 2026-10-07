"""Integração com o Postgres do `docker compose` (`make up`). Pulado se o banco não responder."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.settings import Settings
from app.core.value_objects import JobStatus, JobType, Origin, OutboxState, Target
from app.domain.models import Envelope
from app.infra.job_repository import JobRepository


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """Engine do Postgres local; pula o teste se `make up` não está rodando."""
    engine = create_async_engine(Settings().database_url)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, sqlalchemy.exc.SQLAlchemyError):
        await engine.dispose()
        pytest.skip("Postgres indisponível (rode `make up`)")
    yield engine
    await engine.dispose()


def make_envelope() -> Envelope:
    """Cria um envelope válido com `job_id` novo, para cada teste ter seus próprios dados."""
    return Envelope(
        job_id=uuid4(),
        type=JobType.IO_SLEEP,
        payload={"ms": 1},
        created_at=datetime.now(UTC),
        traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        attempt=0,
        origin=Origin.GATEWAY_PY,
    )


async def delete_job(engine: AsyncEngine, envelope: Envelope) -> None:
    """Remove o job e suas linhas dependentes (ordem respeita as chaves estrangeiras)."""
    async with engine.begin() as conn:
        for table in ("outbox", "job_results", "jobs"):
            await conn.execute(
                text(f"DELETE FROM {table} WHERE job_id = :id"),  # noqa: S608
                {"id": envelope.job_id},
            )


async def test_enqueue_writes_job_and_outbox_with_routing_key(engine: AsyncEngine) -> None:
    """Enfileirar com target específico grava a outbox pendente com a routing key certa."""
    envelope = make_envelope()
    try:
        await JobRepository(engine).enqueue(envelope, Target.CELERY)

        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT routing_key, envelope, published_at FROM outbox WHERE job_id = :id"
                    ),
                    {"id": envelope.job_id},
                )
            ).one()
        assert row.routing_key == "celery"
        assert row.published_at is None
        stored = row.envelope if isinstance(row.envelope, dict) else json.loads(row.envelope)
        assert stored["job_id"] == str(envelope.job_id)
    finally:
        await delete_job(engine, envelope)


async def test_enqueue_failure_leaves_no_orphan_outbox_row(engine: AsyncEngine) -> None:
    """Se a gravação falha no meio, nada fica pela metade.

    O segundo `enqueue` repete o `job_id` e estoura no INSERT de `jobs`; a outbox não pode
    ganhar uma segunda linha. É a garantia de atomicidade do padrão outbox.
    """
    envelope = make_envelope()
    repository = JobRepository(engine)
    try:
        await repository.enqueue(envelope, Target.ALL)
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            await repository.enqueue(envelope, Target.ALL)  # job_id duplicado

        async with engine.connect() as conn:
            count = (
                await conn.execute(
                    text("SELECT count(*) FROM outbox WHERE job_id = :id"), {"id": envelope.job_id}
                )
            ).scalar_one()
        assert count == 1
    finally:
        await delete_job(engine, envelope)


async def test_find_reports_pending_then_completed(engine: AsyncEngine) -> None:
    """O status do job sai de `pending` para `completed` quando o worker grava o resultado."""
    envelope = make_envelope()
    repository = JobRepository(engine)
    try:
        await repository.enqueue(envelope, Target.GO)
        pending = await repository.find(envelope.job_id)
        assert pending is not None
        assert pending.status is JobStatus.PENDING

        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO job_results "
                    "(job_id, worker, status, started_at, finished_at, result) "
                    "VALUES (:id, 'go', 'succeeded', now(), now(), CAST(:result AS jsonb))"
                ),
                {"id": envelope.job_id, "result": '{"slept_ms": 1}'},
            )
        done = await repository.find(envelope.job_id)
        assert done is not None
        assert done.status is JobStatus.COMPLETED
        assert done.results[0].result == {"slept_ms": 1}
    finally:
        await delete_job(engine, envelope)


async def test_list_outbox_filters_by_state(engine: AsyncEngine) -> None:
    """Uma linha recém-criada aparece em `pending` e não aparece em `published`."""
    envelope = make_envelope()
    repository = JobRepository(engine)
    try:
        await repository.enqueue(envelope, Target.ALL)

        pending = await repository.list_outbox(OutboxState.PENDING, 200)
        published = await repository.list_outbox(OutboxState.PUBLISHED, 200)

        assert envelope.job_id in [entry.job_id for entry in pending]
        assert envelope.job_id not in [entry.job_id for entry in published]
    finally:
        await delete_job(engine, envelope)


async def test_list_recent_returns_newest_first_with_results(engine: AsyncEngine) -> None:
    """A listagem devolve o job mais novo antes do mais antigo."""
    older, newer = make_envelope(), make_envelope()
    repository = JobRepository(engine)
    try:
        await repository.enqueue(older, Target.ALL)
        await repository.enqueue(newer, Target.ALL)

        views = await repository.list_recent(200)

        ids = [view.record.job_id for view in views]
        assert ids.index(newer.job_id) < ids.index(older.job_id)
    finally:
        await delete_job(engine, older)
        await delete_job(engine, newer)
