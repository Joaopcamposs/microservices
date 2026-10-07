"""Modelos do benchmark: cenário, workers e resultado de uma rodada."""

from dataclasses import dataclass
from enum import StrEnum


class Worker(StrEnum):
    """Stacks de worker comparadas; o valor é o `target` aceito pelos gateways."""

    ASYNCIO = "asyncio"
    GO = "go"
    CELERY = "celery"
    TASKIQ = "taskiq"

    @property
    def containers(self) -> tuple[str, ...]:
        """Serviços do compose que fazem parte da stack (inclui a bridge, quando existe)."""
        match self:
            case Worker.ASYNCIO:
                return ("worker-asyncio",)
            case Worker.GO:
                return ("worker-go",)
            case Worker.CELERY:
                return ("celery-bridge", "worker-celery")
            case Worker.TASKIQ:
                return ("taskiq-bridge", "worker-taskiq")


@dataclass(frozen=True, slots=True)
class Scenario:
    """Um tipo de carga: qual job enviar, com qual payload e quantos jobs por rodada."""

    name: str
    job_type: str
    payload: dict[str, object]
    jobs: int
    warmup_jobs: int
    description: str


@dataclass(frozen=True, slots=True)
class RoundResult:
    """Métricas de uma rodada (um cenário, uma stack, uma repetição)."""

    scenario: str
    worker: str
    round: int
    jobs: int
    failed: int
    lost: int
    wall_seconds: float
    throughput: float
    exec_p50: float
    exec_p95: float
    exec_p99: float
    e2e_p50: float
    e2e_p95: float
    e2e_p99: float
    outbox_lag_p95: float
    cpu_pct_mean: float
    cpu_pct_max: float
    mem_mb_max: float
