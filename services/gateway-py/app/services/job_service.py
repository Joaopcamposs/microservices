"""Caso de uso do gateway: transformar um pedido HTTP em job persistido e pronto para publicar."""

from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from opentelemetry.trace import SpanKind, Tracer

from app.core.value_objects import JobType, JsonObject, Origin, OutboxState, Target
from app.domain.models import Envelope, JobView, OutboxEntry
from app.domain.payload_validator import PayloadValidator
from app.infra.tracing import current_traceparent


class JobStore(Protocol):
    """Porta de persistência exigida pelo serviço.

    É um `Protocol` para o serviço depender de um contrato e não do Postgres: nos testes
    entra um fake em memória, em produção entra `JobRepository`.
    """

    async def enqueue(self, envelope: Envelope, target: Target) -> None:
        """Persiste o job e a linha da outbox atomicamente."""
        ...

    async def find(self, job_id: UUID) -> JobView | None:
        """Busca um job com resultados, ou `None`."""
        ...

    async def list_recent(self, limit: int) -> list[JobView]:
        """Lista jobs recentes com resultados."""
        ...

    async def list_outbox(self, state: OutboxState, limit: int) -> list[OutboxEntry]:
        """Lista linhas da outbox por estado."""
        ...

    async def ping(self) -> None:
        """Levanta exceção se o armazenamento está indisponível."""
        ...


class JobService:
    """Caso de uso: validar o pedido, montar o envelope e entregá-lo à outbox."""

    def __init__(
        self, store: JobStore, validator: PayloadValidator, origin: Origin, tracer: Tracer
    ) -> None:
        """Recebe as dependências por injeção; `origin` identifica este gateway no envelope."""
        self._store = store
        self._validator = validator
        self._origin = origin
        self._tracer = tracer

    async def submit(self, job_type: JobType, target: Target, payload: JsonObject) -> UUID:
        """Valida o payload, monta o envelope e o grava na outbox; devolve o `job_id`.

        A validação vem antes de qualquer escrita: pedido inválido não deixa rastro no banco.
        O `job_id` nasce aqui (UUID v4) porque é a chave de idempotência dos workers.
        """
        self._validator.validate(job_type, payload)
        # Span PRODUCER: o `traceparent` gravado no envelope é o dele, então relay e workers
        # continuam o trace a partir daqui.
        with self._tracer.start_as_current_span(
            "enqueue job",
            kind=SpanKind.PRODUCER,
            attributes={"job.type": job_type.value, "job.target": target.value},
        ) as span:
            envelope = Envelope(
                job_id=uuid4(),
                type=job_type,
                payload=payload,
                created_at=datetime.now(UTC),
                traceparent=current_traceparent(),
                attempt=0,
                origin=self._origin,
            )
            span.set_attribute("job.id", str(envelope.job_id))
            await self._store.enqueue(envelope, target)
        return envelope.job_id

    async def get(self, job_id: UUID) -> JobView | None:
        """Consulta um job com os resultados por worker."""
        return await self._store.find(job_id)

    async def list_recent(self, limit: int) -> list[JobView]:
        """Lista os jobs mais recentes."""
        return await self._store.list_recent(limit)

    async def list_outbox(self, state: OutboxState, limit: int) -> list[OutboxEntry]:
        """Lista a outbox para depuração do fluxo gateway -> relay."""
        return await self._store.list_outbox(state, limit)

    async def check_health(self) -> None:
        """Verifica a conexão com o armazenamento; levanta exceção se estiver fora."""
        await self._store.ping()
