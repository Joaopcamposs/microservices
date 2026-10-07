"""Configuração do worker TaskIQ e da bridge, lida de variáveis de ambiente `WORKER_*`.

Bridge e worker TaskIQ rodam da mesma imagem e leem a mesma configuração; só o comando muda.
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
    queue: str = "jobs.taskiq"
    # Fila e exchange internos do TaskIQ, para onde a bridge entrega. Ficam fora do
    # definitions.json porque pertencem ao framework: é o TaskIQ quem os declara.
    taskiq_queue: str = "taskiq.jobs"
    # Mensagens entregues sem ack ao mesmo tempo. Igual nas quatro stacks (benchmark justo).
    # Na bridge é o prefetch do consumidor; no TaskIQ, o total em voo é workers x qos.
    prefetch: int = Field(default=64, ge=1)
    # Processos do `taskiq worker --workers N` (2 no README); divide o prefetch entre eles.
    workers: int = Field(default=2, ge=1)
    metrics_port: int = Field(default=9104, ge=1, le=65535)
    contracts_dir: Path = Field(default_factory=_default_contracts_dir)
