"""Contêiner de dependências do processo."""

from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.settings import Settings
from app.core.value_objects import Origin
from app.domain.payload_validator import PayloadValidator
from app.infra.job_repository import JobRepository
from app.services.job_service import JobService


class Services:
    """Dono do estado de processo (engine e serviços), criado e destruído no `lifespan`.

    Concentrar a construção aqui evita estado global em módulo: tudo que tem ciclo de vida
    (pool de conexões) é criado em um lugar e fechado em outro, de forma explícita.
    """

    def __init__(self, settings: Settings, tracer_provider: TracerProvider) -> None:
        """Monta o engine, o validador de schemas e o `JobService` com suas dependências.

        `create_async_engine` não abre conexão aqui; o pool conecta sob demanda. O provider de
        traces nasce fora (a instrumentação HTTP precisa dele antes do lifespan), mas o
        encerramento é daqui.
        """
        self._engine: AsyncEngine = create_async_engine(settings.database_url)
        self.tracer_provider: TracerProvider = tracer_provider
        validator = PayloadValidator.from_directory(settings.contracts_dir / "jobs")
        self.job_service = JobService(
            JobRepository(self._engine),
            validator,
            Origin.GATEWAY_PY,
            self.tracer_provider.get_tracer("gateway-py"),
        )

    async def close(self) -> None:
        """Fecha o pool e descarrega os spans pendentes; chamado no shutdown da aplicação."""
        await self._engine.dispose()
        self.tracer_provider.shutdown()
