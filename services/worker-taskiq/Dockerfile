# Imagem única do worker-taskiq: serve a bridge e o worker TaskIQ (o comando muda no compose).
# O contexto de build é a raiz do repositório porque a imagem também precisa de `contracts/`.
FROM python:3.13-slim
# uv gerencia as dependências a partir do uv.lock (build reprodutível).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
# Dependências antes do código: o cache do Docker só invalida esta camada se elas mudarem.
COPY services/worker-taskiq/pyproject.toml services/worker-taskiq/uv.lock ./
RUN uv sync --frozen --no-dev

# Código e contratos compartilhados; WORKER_CONTRACTS_DIR aponta o app para eles.
COPY services/worker-taskiq/app ./app
COPY contracts /contracts
ENV WORKER_CONTRACTS_DIR=/contracts

# Padrão: o worker TaskIQ. A bridge sobe com `python -m app.bridge_main` (ver docker-compose.yml).
# O venv do uv entra no PATH e os comandos rodam direto, sem `uv run` (que tentaria escrever em
# cache). Usuário sem privilégios, por boa prática.
ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --no-create-home worker
USER worker
CMD ["taskiq", "worker", "app.taskiq_app:broker", "--workers", "2", "--max-async-tasks", "32"]
