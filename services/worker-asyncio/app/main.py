"""Ponto de entrada do worker-asyncio: `python -m app.main`."""

import asyncio
import logging
import signal

from prometheus_client import start_http_server

from app.core.settings import Settings
from app.services.container import Services


async def run() -> None:
    """Sobe métricas e consumidor e roda até SIGINT/SIGTERM, encerrando de forma ordenada."""
    settings = Settings()
    services = await Services.create(settings)
    # Servidor HTTP em thread própria, só para o scrape do Prometheus.
    start_http_server(settings.metrics_port, registry=services.registry)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await services.consumer.run(stop)
    finally:
        await services.close()


def main() -> None:
    """Configura o log e executa o laço principal."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())


if __name__ == "__main__":
    main()
