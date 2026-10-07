"""Contêiner de dependências do processo."""

import asyncpg
from prometheus_client import CollectorRegistry

from app.core.settings import Settings
from app.domain.contracts import ContractValidator
from app.domain.handlers import build_handlers
from app.infra.consumer import QueueConsumer
from app.infra.result_repository import ResultRepository
from app.services.job_processor import JobProcessor
from app.services.metrics import WorkerMetrics


class Services:
    """Dono do estado de processo (pool, métricas, consumidor), criado no `main`.

    Evita estado global em módulo: o que tem ciclo de vida nasce em `create` e morre em `close`.
    """

    def __init__(
        self, pool: asyncpg.Pool, registry: CollectorRegistry, consumer: QueueConsumer
    ) -> None:
        """Recebe as peças já montadas; use `create` para construí-las a partir do ambiente."""
        self._pool = pool
        self.registry = registry
        self.consumer = consumer

    @classmethod
    async def create(cls, settings: Settings) -> "Services":
        """Abre o pool do Postgres e monta processor e consumidor com suas dependências."""
        pool = await asyncpg.create_pool(settings.database_url)
        registry = CollectorRegistry()
        metrics = WorkerMetrics(registry)
        processor = JobProcessor(
            ContractValidator.from_directory(settings.contracts_dir),
            build_handlers(),
            ResultRepository(pool),
            metrics,
        )
        consumer = QueueConsumer(
            settings.amqp_url, settings.queue, settings.prefetch, processor, metrics
        )
        return cls(pool, registry, consumer)

    async def close(self) -> None:
        """Fecha o pool de conexões no shutdown."""
        await self._pool.close()
