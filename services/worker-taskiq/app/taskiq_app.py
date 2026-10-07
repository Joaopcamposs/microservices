"""Broker TaskIQ e a task que processa um job: `taskiq worker app.taskiq_app:broker`.

O TaskIQ precisa de uma instância de módulo (o CLI a importa por nome), então `broker` é a
única exceção à regra de "sem estado global". As dependências da task (validador, pool do
Postgres) ficam no `state` do worker, criado no evento de startup e fechado no shutdown.
"""

import logging
from typing import Annotated

import asyncpg
from taskiq import TaskiqDepends, TaskiqEvents, TaskiqState
from taskiq.middlewares import SimpleRetryMiddleware
from taskiq_aio_pika import AioPikaBroker, Exchange, Queue
from taskiq_aio_pika.queue import QueueType

from app.core.settings import Settings
from app.domain.contracts import ContractValidator, InvalidMessageError
from app.domain.handlers import build_handlers
from app.infra.result_repository import ResultRepository
from app.services.job_processor import JobProcessor

logger = logging.getLogger(__name__)

# Nome estável da task: a bridge a chama pelo objeto `process_job`, que o registra com este nome.
TASK_NAME = "jobs.process"

_settings = Settings()

broker = AioPikaBroker(
    url=_settings.amqp_url,
    # Prefetch dividido entre os processos do worker: 2 x 32 = 64, o mesmo das outras stacks.
    qos=max(1, _settings.prefetch // _settings.workers),
    exchange=Exchange(name=_settings.taskiq_queue),
    # Fila clássica (o padrão do TaskIQ é quorum) para a comparação com o Celery usar o mesmo
    # tipo de fila do RabbitMQ.
    task_queues=[Queue(name=_settings.taskiq_queue, type=QueueType.CLASSIC)],
).with_middlewares(
    # Banco fora é falha de infra: repete em vez de perder o resultado. Esgotadas as tentativas
    # a task falha e fica no log (cenário de falha detalhado na fase 7).
    SimpleRetryMiddleware(default_retry_count=5),
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def create_processor(state: TaskiqState) -> None:
    """Monta pool e processor uma vez por processo do worker, no startup."""
    state.pool = await asyncpg.create_pool(_settings.database_url)
    state.processor = JobProcessor(
        ContractValidator.from_directory(_settings.contracts_dir),
        build_handlers(),
        ResultRepository(state.pool),
    )


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def close_pool(state: TaskiqState) -> None:
    """Fecha o pool do Postgres no encerramento do worker."""
    await state.pool.close()


@broker.task(task_name=TASK_NAME, retry_on_error=True, max_retries=5)
async def process_job(body: str, state: Annotated[TaskiqState, TaskiqDepends()]) -> None:
    """Processa o envelope (JSON em texto) que a bridge entregou.

    Recebe `str` e não `dict` para o JSON chegar ao `ContractValidator` exatamente como veio
    do RabbitMQ, sem passar pela serialização do TaskIQ.
    """
    try:
        await state.processor.process(body.encode())
    except InvalidMessageError:
        # A bridge já valida; se chegou aqui o contrato mudou entre os dois. Repetir não ajuda.
        logger.exception("mensagem inválida chegou à task, descartada")
