"""Caso de uso: transforma o corpo de uma mensagem em um resultado gravado."""

import logging
from datetime import UTC, datetime

from app.core.value_objects import WORKER_NAME, JobType, ResultStatus
from app.domain.contracts import ContractValidator, InvalidMessageError
from app.domain.handlers import Handler
from app.domain.models import Job, JobResult
from app.infra.result_repository import ResultRepository
from app.services.metrics import WorkerMetrics

logger = logging.getLogger(__name__)


class JobProcessor:
    """Valida, executa o handler e grava o resultado.

    Falha do **handler** é resultado (`failed` gravado e mensagem confirmada): reexecutar o
    mesmo erro não ajuda. Falha de **infraestrutura** (banco fora) propaga como exceção para
    o consumidor decidir o ack, porque nada foi gravado.
    """

    def __init__(
        self,
        validator: ContractValidator,
        handlers: dict[JobType, Handler],
        repository: ResultRepository,
        metrics: WorkerMetrics,
    ) -> None:
        """Recebe as dependências por injeção, sem criar nada sozinho."""
        self._validator = validator
        self._handlers = handlers
        self._repository = repository
        self._metrics = metrics

    async def process(self, body: bytes) -> None:
        """Processa uma mensagem; levanta `InvalidMessageError` se ela nunca poderá passar."""
        job = self._validator.parse(body)
        handler = self._handlers.get(job.type)
        if handler is None:
            raise InvalidMessageError(f"sem handler para o tipo {job.type.value}")
        result = await self._run(job, handler)
        saved = await self._repository.save(result)
        status = result.status.value if saved else "duplicate"
        self._metrics.processed.labels(status=status).inc()
        logger.info("job %s: %s", job.job_id, status, extra={"traceparent": job.traceparent})

    async def _run(self, job: Job, handler: Handler) -> JobResult:
        """Executa o handler e converte sucesso ou exceção em `JobResult`."""
        started_at = datetime.now(UTC)
        try:
            output = await handler(job.payload)
            status, error = ResultStatus.SUCCEEDED, None
        except Exception as exc:  # noqa: BLE001 - qualquer erro do handler vira resultado `failed`
            output, status, error = None, ResultStatus.FAILED, f"{type(exc).__name__}: {exc}"
        finished_at = datetime.now(UTC)
        self._metrics.duration.observe((finished_at - started_at).total_seconds())
        return JobResult(job.job_id, WORKER_NAME, status, started_at, finished_at, output, error)
