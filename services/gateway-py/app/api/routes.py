"""Rotas HTTP do gateway. Cada rota só traduz HTTP para chamadas do `JobService`."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status

from app.api.schemas import JobAccepted, JobOut, OutboxEntryOut, WorkerResultOut
from app.core.value_objects import JobType, JsonObject, OutboxState, Target
from app.domain.models import JobView
from app.services.job_service import JobService

router = APIRouter()

# Exemplos exibidos no Swagger ("Try it out"); uma entrada por tipo de job com schema.
PAYLOAD_EXAMPLES = {
    "io.sleep": {
        "summary": "io.sleep: dorme 100 ms",
        "description": "Use com `type=io.sleep`. Mede só o overhead do framework.",
        "value": {"ms": 100},
    },
    "cpu.pbkdf2": {
        "summary": "cpu.pbkdf2: 100 mil iterações",
        "description": 'Use com `type=cpu.pbkdf2`. Job CPU-bound; devolve `{"digest": ...}`.',
        "value": {"password": "senha", "salt": "sal", "iterations": 100000},
    },
    "io.fetch_urls": {
        "summary": "io.fetch_urls: 2 URLs do mock-server",
        "description": (
            "Use com `type=io.fetch_urls`. O `mock-server` (profiles `python`, `go`, `all`) "
            "responde `/delay/{ms}` e `/status/{code}`."
        ),
        "value": {
            "urls": ["http://mock-server:8090/delay/200", "http://mock-server:8090/status/404"]
        },
    },
    "data.json_transform": {
        "summary": "data.json_transform: 1000 registros",
        "description": "Use com `type=data.json_transform`. Gera, serializa, lê e agrega JSON.",
        "value": {"records": 1000, "seed": 42},
    },
}


def get_job_service(request: Request) -> JobService:
    """Dependência do FastAPI: entrega o `JobService` criado no `lifespan`.

    Existe como função para os testes substituírem o serviço via `dependency_overrides`.
    """
    job_service: JobService = request.app.state.services.job_service
    return job_service


# Atalho de tipo para injetar o serviço nas rotas.
JobServiceDep = Annotated[JobService, Depends(get_job_service)]


def to_job_out(view: JobView) -> JobOut:
    """Converte o modelo de domínio no modelo de resposta da API."""
    return JobOut(
        job_id=view.record.job_id,
        type=view.record.type,
        target=view.record.target,
        origin=view.record.origin,
        created_at=view.record.created_at,
        status=view.status,
        results=[
            WorkerResultOut.model_validate(result, from_attributes=True) for result in view.results
        ],
    )


@router.post(
    "/jobs",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["jobs"],
    summary="Enfileira um job",
    description=(
        "Valida o payload contra `contracts/jobs/<type>.schema.json` e grava o job e a outbox "
        "na mesma transação. Quem publica no RabbitMQ é o `outbox-relay`. "
        "`target=all` usa fanout (4 workers); os demais, um worker só."
    ),
)
async def submit_job(
    service: JobServiceDep,
    payload: Annotated[JsonObject, Body(openapi_examples=PAYLOAD_EXAMPLES)],
    job_type: Annotated[JobType, Query(alias="type", description="Tipo do job.")],
    target: Annotated[Target, Query(description="Stack que processa o job.")] = Target.ALL,
) -> JobAccepted:
    """Recebe o pedido, delega ao serviço e responde `202` com o `job_id`."""
    job_id = await service.submit(job_type, target, payload)
    return JobAccepted(job_id=job_id)


@router.get(
    "/jobs",
    tags=["jobs"],
    summary="Lista os jobs mais recentes",
    description="Atalho para achar `job_id`s e ver o status de cada job sem abrir o banco.",
)
async def list_jobs(
    service: JobServiceDep,
    limit: Annotated[int, Query(ge=1, le=200, description="Máximo de jobs.")] = 20,
) -> list[JobOut]:
    """Lista os jobs mais recentes (até `limit`)."""
    return [to_job_out(view) for view in await service.list_recent(limit)]


@router.get(
    "/jobs/{job_id}",
    tags=["jobs"],
    summary="Consulta um job",
    description="Status agregado e resultado de cada worker.",
)
async def get_job(job_id: UUID, service: JobServiceDep) -> JobOut:
    """Devolve o job ou `404` se o `job_id` não existe."""
    view = await service.get(job_id)
    if view is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job não encontrado")
    return to_job_out(view)


@router.get(
    "/outbox",
    tags=["debug"],
    summary="Inspeciona a outbox",
    description=(
        "Linhas aguardando o relay (`pending`) ou já publicadas (`published`). "
        "Útil para conferir se o job saiu do gateway e se o relay está esvaziando a fila."
    ),
)
async def list_outbox(
    service: JobServiceDep,
    state: Annotated[OutboxState, Query(description="Filtro por estado.")] = OutboxState.PENDING,
    limit: Annotated[int, Query(ge=1, le=200, description="Máximo de linhas.")] = 50,
) -> list[OutboxEntryOut]:
    """Lista linhas da outbox no estado pedido (padrão: aguardando o relay)."""
    entries = await service.list_outbox(state, limit)
    return [OutboxEntryOut.model_validate(entry, from_attributes=True) for entry in entries]


@router.get("/healthz", tags=["ops"], summary="Liveness e conexão com o Postgres")
async def healthz(service: JobServiceDep) -> dict[str, str]:
    """Responde `200` se o Postgres atende e `503` caso contrário."""
    try:
        await service.check_health()
    except Exception as error:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "banco indisponível") from error
    return {"status": "ok"}
