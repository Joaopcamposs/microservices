"""Aplicação Celery e a task que processa um job: `celery -A app.celery_app worker`.

O Celery precisa de uma instância de módulo (o CLI a importa por nome), então `celery` é a
única exceção à regra de "sem estado global". As dependências da task (validador, pool do
Postgres) não ficam em módulo: nascem uma vez por processo filho, em `JobTask.processor`.
"""

import logging

import psycopg
from celery import Celery, Task
from psycopg_pool import ConnectionPool

from app.core.settings import Settings
from app.domain.contracts import ContractValidator, InvalidMessageError
from app.domain.handlers import build_handlers
from app.infra.result_repository import ResultRepository
from app.services.job_processor import JobProcessor

logger = logging.getLogger(__name__)

# Nome estável da task: a bridge a chama por nome, sem importar o código do worker.
TASK_NAME = "jobs.process"

_settings = Settings()

celery = Celery("worker-celery", broker=_settings.amqp_url)
celery.conf.update(
    task_default_queue=_settings.celery_queue,
    worker_concurrency=_settings.concurrency,
    # Total em voo = concurrency x multiplicador: 4 x 16 = 64, o mesmo prefetch das outras stacks.
    worker_prefetch_multiplier=max(1, _settings.prefetch // _settings.concurrency),
    # ack tardio: a mensagem só sai da fila depois da task terminar; se o processo morrer no
    # meio, o broker reentrega e a gravação idempotente absorve a duplicata.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # O resultado vai para `job_results` (Postgres), não para um result backend do Celery.
    task_ignore_result=True,
    # `confirm_publish`: a bridge só confirma a mensagem original depois do broker aceitar a
    # task, senão uma queda do broker no meio perderia o job.
    broker_transport_options={"confirm_publish": True},
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,
)


class JobTask(Task):
    """Task base que monta o `JobProcessor` uma vez por processo filho, de forma preguiçosa.

    O pool prefork faz `fork`: conexões criadas antes dele seriam compartilhadas entre
    processos. Criar o pool no primeiro uso garante um por filho. É o idioma recomendado na
    documentação do Celery para recursos por processo.
    """

    _processor: JobProcessor | None = None

    @property
    def processor(self) -> JobProcessor:
        """Devolve o processor deste processo, criando-o no primeiro acesso."""
        if self._processor is None:
            pool = ConnectionPool(
                _settings.database_url, min_size=1, max_size=1, timeout=5, open=True
            )
            self._processor = JobProcessor(
                ContractValidator.from_directory(_settings.contracts_dir),
                build_handlers(),
                ResultRepository(pool),
            )
        return self._processor


@celery.task(
    base=JobTask,
    bind=True,
    name=TASK_NAME,
    # Banco fora é falha de infra: repete com backoff em vez de perder o resultado. Esgotadas as
    # tentativas a task falha e fica no log (cenário de falha detalhado na fase 7).
    autoretry_for=(psycopg.OperationalError,),
    retry_backoff=True,
    max_retries=5,
)
def process_job(self: JobTask, body: str) -> None:
    """Processa o envelope (JSON em texto) que a bridge entregou.

    Recebe `str` e não `dict` para o JSON chegar ao `ContractValidator` exatamente como veio
    do RabbitMQ, sem passar pela serialização do Celery.
    """
    try:
        self.processor.process(body.encode())
    except InvalidMessageError:
        # A bridge já valida; se chegou aqui o contrato mudou entre os dois. Repetir não ajuda.
        logger.exception("mensagem inválida chegou à task, descartada")
