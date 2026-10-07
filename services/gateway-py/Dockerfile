# Imagem do gateway-py. O contexto de build é a raiz do repositório (ver docker-compose.yml),
# porque a imagem também precisa de `contracts/`, que fica fora de services/gateway-py.
FROM python:3.13-slim
# uv gerencia as dependências a partir do uv.lock (build reprodutível).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
# Dependências antes do código: o cache do Docker só invalida esta camada se elas mudarem.
COPY services/gateway-py/pyproject.toml services/gateway-py/uv.lock ./
RUN uv sync --frozen --no-dev

# Código e contratos compartilhados; GATEWAY_CONTRACTS_DIR aponta o app para eles.
COPY services/gateway-py/app ./app
COPY contracts /contracts
ENV GATEWAY_CONTRACTS_DIR=/contracts

# `--no-sync`: não reinstala dependências ao iniciar.
CMD ["uv", "run", "--no-sync", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
