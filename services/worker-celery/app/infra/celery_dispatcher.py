"""Entrega o envelope ao Celery: o hop final da bridge."""

from celery import Celery


class CeleryDispatcher:
    """Publica a task do Celery com confirmação do broker.

    Chama a task **por nome** (`send_task`): a bridge não importa o código do worker, só conhece
    o contrato da task. A chamada é bloqueante (kombu síncrono), por isso a bridge a executa
    em thread.
    """

    def __init__(self, app: Celery, task_name: str) -> None:
        """Recebe a aplicação Celery e o nome da task que processa o job."""
        self._app = app
        self._task_name = task_name

    def submit(self, body: bytes) -> None:
        """Enfileira a task com o envelope em texto; levanta se o broker não confirmar."""
        self._app.send_task(self._task_name, args=[body.decode()])
