"""Vocabulário fixo do worker, alinhado ao contrato e às constraints do banco."""

from enum import StrEnum

# Alias para JSON arbitrário: o payload de cada job é validado por JSON Schema em `contracts/jobs`.
type JsonObject = dict[str, object]

# Nome gravado em `job_results.worker`; a constraint do banco só aceita os quatro workers.
WORKER_NAME = "asyncio"


class JobType(StrEnum):
    """Tipos de job do contrato (`contracts/envelope.schema.json`)."""

    IO_SLEEP = "io.sleep"
    IO_FETCH_URLS = "io.fetch_urls"
    CPU_PBKDF2 = "cpu.pbkdf2"
    DATA_JSON_TRANSFORM = "data.json_transform"
    PIPELINE_FANOUT = "pipeline.fanout"


class ResultStatus(StrEnum):
    """Resultado final do worker para um job (`job_results.status`)."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
