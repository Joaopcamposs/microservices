"""Métricas Prometheus da bridge, num registry próprio para não depender de estado global."""

from prometheus_client import CollectorRegistry, Counter


class BridgeMetrics:
    """Contador de mensagens da bridge, exposto em `/metrics` (porta própria).

    O worker Celery (prefork, vários processos) não expõe métricas nesta fase: o modo multiprocess
    do prometheus-client pede configuração própria e entra com a observabilidade (fase 5).
    """

    def __init__(self, registry: CollectorRegistry) -> None:
        """Registra as métricas no `registry` recebido."""
        self.forwarded = Counter(
            "bridge_messages_total",
            "Mensagens lidas da fila jobs.*, por destino (forwarded, rejected, requeued).",
            ["outcome"],
            registry=registry,
        )
