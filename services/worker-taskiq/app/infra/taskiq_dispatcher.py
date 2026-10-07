"""Entrega o envelope ao TaskIQ: o hop final da bridge."""

from taskiq import AsyncTaskiqDecoratedTask


class TaskiqDispatcher:
    """Publica a task do TaskIQ no broker (o broker precisa estar com `startup()` feito).

    Diferente do Celery, o `kiq` é assíncrono: a bridge o aguarda direto no event loop, sem
    thread. O aio-pika do broker já espera o publish ser aceito pelo RabbitMQ.
    """

    def __init__(self, task: AsyncTaskiqDecoratedTask[..., None]) -> None:
        """Recebe a task que processa o job."""
        self._task = task

    async def submit(self, body: bytes) -> None:
        """Enfileira a task com o envelope em texto; levanta se o broker recusar."""
        await self._task.kiq(body.decode())
