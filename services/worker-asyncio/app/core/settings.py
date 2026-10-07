"""Configuração do worker, lida de variáveis de ambiente `WORKER_*`.

Mantém a configuração fora do código: o mesmo worker roda local e no Docker mudando só o ambiente.
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

    # DSN do Postgres no formato nativo do asyncpg (sem o prefixo `+asyncpg` do SQLAlchemy).
    database_url: str = "postgresql://postgres:lab@localhost:5432/lab"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    # Fila própria desta stack; o binding vem de infra/rabbitmq/definitions.json.
    queue: str = "jobs.asyncio"
    # Mensagens entregues sem ack ao mesmo tempo. Igual nas quatro stacks (benchmark justo).
    prefetch: int = Field(default=64, ge=1)
    metrics_port: int = Field(default=9101, ge=1, le=65535)
    contracts_dir: Path = Field(default_factory=_default_contracts_dir)
