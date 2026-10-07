# Imagem do worker-asyncio. O contexto de build é a raiz do repositório (ver docker-compose.yml),
# porque a imagem também precisa de `contracts/`, que fica fora de services/worker-asyncio.
FROM python:3.13-slim
# uv gerencia as dependências a partir do uv.lock (build reprodutível).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
# Dependências antes do código: o cache do Docker só invalida esta camada se elas mudarem.
COPY services/worker-asyncio/pyproject.toml services/worker-asyncio/uv.lock ./
RUN uv sync --frozen --no-dev

# Código e contratos compartilhados; WORKER_CONTRACTS_DIR aponta o app para eles.
COPY services/worker-asyncio/app ./app
COPY contracts /contracts
ENV WORKER_CONTRACTS_DIR=/contracts

# `--no-sync`: não reinstala dependências ao iniciar.
CMD ["uv", "run", "--no-sync", "python", "-m", "app.main"]
