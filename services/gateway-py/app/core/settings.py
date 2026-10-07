"""Configuração do gateway, lida de variáveis de ambiente.

Mantém a configuração fora do código: o mesmo binário roda local e no Docker mudando só o ambiente.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_contracts_dir() -> Path:
    """Devolve o caminho de `contracts/` no monorepo, para rodar fora do Docker.

    É uma `default_factory` de propósito: só executa quando `GATEWAY_CONTRACTS_DIR` não está
    definida. Dentro da imagem o caminho do repositório não existe, então calcular isso
    sempre quebraria o boot.
    """
    return Path(__file__).resolve().parents[4] / "contracts"


class Settings(BaseSettings):
    """Variáveis `GATEWAY_*`. Os padrões servem para desenvolvimento com `make up`."""

    model_config = SettingsConfigDict(env_prefix="GATEWAY_")

    # URL do Postgres no formato SQLAlchemy async (driver asyncpg).
    database_url: str = "postgresql+asyncpg://postgres:lab@localhost:5432/lab"
    # Pasta com os schemas compartilhados entre os serviços (envelope e payloads).
    contracts_dir: Path = Field(default_factory=_default_contracts_dir)
