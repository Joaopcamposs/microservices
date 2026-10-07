"""Ponto de entrada da bridge do Celery: `python -m app.bridge_main`."""

import asyncio
import logging
import signal

from prometheus_client import CollectorRegistry, start_http_server

from app.celery_app import TASK_NAME, celery
from app.core.settings import Settings
from app.domain.contracts import ContractValidator
from app.domain.handlers import build_handlers
from app.infra.bridge import Bridge
from app.infra.celery_dispatcher import CeleryDispatcher
from app.services.bridge_metrics import BridgeMetrics


async def run() -> None:
    """Monta a bridge e roda até SIGINT/SIGTERM, encerrando de forma ordenada."""
    settings = Settings()
    registry = CollectorRegistry()
    bridge = Bridge(
        settings.amqp_url,
        settings.queue,
        settings.prefetch,
        ContractValidator.from_directory(settings.contracts_dir),
        frozenset(build_handlers()),
        CeleryDispatcher(celery, TASK_NAME),
        BridgeMetrics(registry),
    )
    # Servidor HTTP em thread própria, só para o scrape do Prometheus.
    start_http_server(settings.metrics_port, registry=registry)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await bridge.run(stop)


def main() -> None:
    """Configura o log e executa o laço principal."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())


if __name__ == "__main__":
    main()
