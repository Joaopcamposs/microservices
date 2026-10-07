"""Configuração do worker Celery e da bridge, lida de variáveis de ambiente `WORKER_*`.

Bridge e worker Celery rodam da mesma imagem e leem a mesma configuração; só o comando muda.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_contracts_dir() -> Path:
    """Devolve o caminho de `contracts/` no monorepo, para rodar fora do Docker.

    É uma `default_factory` de propósito: dentro da imagem o caminho do repositório não existe,
    então só calculamos quando `WORKER_CONTRACTS_DIR` não está definida.
    """
    return Path(__file__).resolve().parents[4] / "contracts"


class Settings(BaseSettings):
    """Variáveis `WORKER_*`. Os padrões servem para desenvolvimento com `make up`."""

    model_config = SettingsConfigDict(env_prefix="WORKER_")

    # DSN do Postgres no formato do libpq/psycopg.
    database_url: str = "postgresql://postgres:lab@localhost:5432/lab"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    # Fila própria desta stack (entrada da bridge); o binding vem de definitions.json.
    queue: str = "jobs.celery"
    # Fila interna do Celery, para onde a bridge entrega. Fica fora do definitions.json porque
    # pertence ao framework: é o Celery quem a declara.
    celery_queue: str = "celery.jobs"
    # Mensagens entregues sem ack ao mesmo tempo. Igual nas quatro stacks (benchmark justo).
    # Na bridge é o prefetch do consumidor; no Celery, o total em voo é concurrency x multiplicador.
    prefetch: int = Field(default=64, ge=1)
    # Processos filhos do pool prefork (`-c 4` do README).
    concurrency: int = Field(default=4, ge=1)
    metrics_port: int = Field(default=9103, ge=1, le=65535)
    contracts_dir: Path = Field(default_factory=_default_contracts_dir)
