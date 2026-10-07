"""Testes da borda HTTP com um serviço falso: rotas, códigos de status e validação de query."""

from collections.abc import Iterator
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.api.routes import get_job_service
from app.core.value_objects import JobType, JsonObject, Target
from app.main import create_app


class FakeJobService:
    """Substitui `JobService` e registra os pedidos recebidos pela rota."""

    def __init__(self) -> None:
        """Começa sem nenhum pedido registrado."""
        self.submitted: list[tuple[JobType, Target, JsonObject]] = []

    async def submit(self, job_type: JobType, target: Target, payload: JsonObject) -> UUID:
        """Guarda o pedido e devolve um `job_id` fixo e previsível."""
        self.submitted.append((job_type, target, payload))
        return UUID(int=1)


@pytest.fixture
def service() -> FakeJobService:
    """Serviço falso compartilhado entre o `client` e as asserções do teste."""
    return FakeJobService()


@pytest.fixture
def client(service: FakeJobService) -> Iterator[TestClient]:
    """Cliente HTTP da aplicação real, com o `JobService` trocado pelo falso."""
    app = create_app()
    app.dependency_overrides[get_job_service] = lambda: service
    with TestClient(app) as test_client:
        yield test_client


def test_post_job_returns_202_and_defaults_target_to_all(
    client: TestClient, service: FakeJobService
) -> None:
    """`POST /jobs` responde 202 e, sem `target`, usa `all` (fanout para os 4 workers)."""
    response = client.post("/jobs", params={"type": "io.sleep"}, json={"ms": 5})

    assert response.status_code == 202
    assert response.json() == {"job_id": str(UUID(int=1))}
    assert service.submitted == [(JobType.IO_SLEEP, Target.ALL, {"ms": 5})]


def test_post_job_rejects_unknown_target(client: TestClient) -> None:
    """Um `target` fora do enum é recusado com 422 antes de chegar ao serviço."""
    response = client.post("/jobs", params={"type": "io.sleep", "target": "rust"}, json={"ms": 5})

    assert response.status_code == 422
