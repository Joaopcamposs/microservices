"""Vocabulário fixo do domínio: valores que o código inteiro compartilha.

Estes enums espelham as listas fechadas do contrato (`contracts/envelope.schema.json`) e das
constraints do banco (`db/migrations/001_init.sql`). Mudou um lado, mude os outros.
"""

from enum import StrEnum

# Alias para JSON arbitrário: o payload de cada job é validado por JSON Schema em `contracts/jobs`.
type JsonObject = dict[str, object]


class JobType(StrEnum):
    """Tipos de job do contrato. Só os que têm schema em `contracts/jobs` são aceitos de fato."""

    IO_SLEEP = "io.sleep"
    IO_FETCH_URLS = "io.fetch_urls"
    CPU_PBKDF2 = "cpu.pbkdf2"
    DATA_JSON_TRANSFORM = "data.json_transform"
    PIPELINE_FANOUT = "pipeline.fanout"


class Target(StrEnum):
    """Stack que deve processar o job: `all` usa o exchange fanout, as demais o direct."""

    ALL = "all"
    CELERY = "celery"
    TASKIQ = "taskiq"
    ASYNCIO = "asyncio"
    GO = "go"


class Origin(StrEnum):
    """Gateway que recebeu o pedido; permite comparar os dois gateways no benchmark."""

    GATEWAY_PY = "gateway-py"
    GATEWAY_GO = "gateway-go"


class ResultStatus(StrEnum):
    """Resultado final de um worker para um job."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class OutboxState(StrEnum):
    """Filtro de consulta da outbox: aguardando o relay, já publicada ou ambas."""

    PENDING = "pending"
    PUBLISHED = "published"
    ALL = "all"


class JobStatus(StrEnum):
    """Estado agregado de um job, derivado da quantidade de resultados recebidos."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"


# Fanout entrega o job às 4 stacks; um target específico, a uma só.
WORKERS_PER_FANOUT = 4
