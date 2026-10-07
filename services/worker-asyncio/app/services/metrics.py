"""Métricas Prometheus do worker, num registry próprio para não depender de estado global."""

from prometheus_client import CollectorRegistry, Counter, Histogram


class WorkerMetrics:
    """Contadores e histograma de processamento, expostos em `/metrics` (porta própria)."""

    def __init__(self, registry: CollectorRegistry) -> None:
        """Registra as métricas no `registry` recebido."""
        self.processed = Counter(
            "worker_jobs_processed_total",
            "Jobs processados, por status (succeeded, failed, duplicate, rejected).",
            ["status"],
            registry=registry,
        )
        # Tempo só do handler (sem fila nem banco): separa o custo do trabalho do do framework.
        self.duration = Histogram(
            "worker_job_duration_seconds",
            "Duração do handler.",
            registry=registry,
        )
