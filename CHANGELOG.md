# Changelog

Registro da evolução do código, no formato [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/). Um único arquivo, com a entrada mais recente no topo. Cada entrada usa a data e a fase do plano (seção 11 do `README.md`) e agrupa por `Adicionado`, `Alterado`, `Corrigido` e `Removido`. Escreva o **porquê** quando a decisão não for óbvia.

## [Não lançado]

### 2026-10-07 · Documentação do código (gateway-py)

#### Alterado
- Docstring de módulo em todo arquivo e docstring de propósito em toda classe, função e método do `gateway-py`, inclusive nos testes; comentários com o porquê nas queries SQL, constantes e decisões não óbvias.
- Comentários explicativos no `Makefile`, `docker-compose.yml` e `Dockerfile`.
- `ruff` passa a exigir docstrings (regras `D1`), então a convenção é verificada pelo `make ruff`.
- `AGENTS.md`: regra explícita de código em inglês, docstrings e comentários em português, e código autoexplicativo para estudo.

### 2026-10-07 · Fase 1 (caminho Python): gateway-py

#### Adicionado
- `services/gateway-py` (FastAPI, SQLAlchemy async, asyncpg) com `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `GET /outbox`, `GET /healthz` e `/metrics`.
- `POST /jobs` valida o payload contra `contracts/jobs/<type>.schema.json` e grava `jobs` + `outbox` na mesma transação. O gateway nunca publica no RabbitMQ.
- `contracts/jobs/io.sleep.schema.json`, primeiro tipo de job com schema.
- Swagger (`/docs`) como interface de teste: descrições, tags e exemplo de payload; `GET /jobs` e `GET /outbox` para consulta sem curl.
- `Dockerfile` do gateway e serviço `gateway-py` no compose (profile `python`).
- Testes: validador de payload, envelope contra `envelope.schema.json`, API e repositório contra o Postgres real (atomicidade da outbox, `pending` → `completed`, listagens).

#### Alterado
- `Makefile`: `run` sobe o gateway com uvicorn (apontava para um `main.py` inexistente); `ruff` e `test` passam a rodar em `services/gateway-py`.
- `AGENTS.md`: regra de consulta via Swagger e regra de manter este changelog.

#### Corrigido
- Caminho padrão de `contracts/` falhava dentro da imagem Docker (`IndexError` em `parents[4]`); agora o padrão só é calculado quando `GATEWAY_CONTRACTS_DIR` não está definido.

#### Notas
- O `traceparent` é gerado localmente por enquanto. Passa a vir do span OpenTelemetry na fase 5.
- Linhas da `outbox` ficam `pending` até o `outbox-relay` existir.

### 2026-10-07 · Fase 0 (fundação)

#### Adicionado
- `docker-compose.yml` com `rabbitmq` e `postgres` (healthchecks).
- `contracts/envelope.schema.json`: envelope neutro com 7 campos.
- `db/migrations/001_init.sql`: tabelas `jobs`, `job_results` (chave `(job_id, worker)`, grava de forma idempotente) e `outbox` (índice parcial de pendentes).
- `infra/rabbitmq/definitions.json`: exchanges `jobs` (fanout), `jobs.direct` e `jobs.dlx`; filas `jobs.<stack>` e `jobs.dlq`. Carregado no boot, então nenhum serviço declara fila.
- `Makefile`: `up`, `down`, `reset`, `logs`, `psql`.

### 2026-10-07 · Planejamento

#### Adicionado
- `README.md` com propósito, arquitetura, jobs, cenários de teste, metodologia de benchmark e fases.
- Padrão **transactional outbox** na publicação, com `outbox-relay` único para os dois gateways (decisão: polling em vez de CDC, para não puxar Debezium/Kafka para a infra).
- `AGENTS.md` com regras de código, padrões do projeto, testes e fluxo de trabalho.
