# Changelog

Registro da evolução do código, no formato [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/). Um único arquivo, com a entrada mais recente no topo. Cada entrada usa a data e a fase do plano (seção 11 do `README.md`) e agrupa por `Adicionado`, `Alterado`, `Corrigido` e `Removido`. Escreva o **porquê** quando a decisão não for óbvia.

## [Não lançado]

### 2026-10-07 · Fase 2 (caminho Go): gateway-go

#### Adicionado
- `services/gateway-go` (Gin, pgx): mesma API do gateway-py (`POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `GET /outbox`, `GET /healthz`, `/metrics`), mesma transação `jobs` + `outbox` e mesmos códigos e formato de erro (`422`/`404` com `{"detail": ...}`). Envelope com `origin: gateway-go`.
- Swagger UI em `/docs` (mesmo endereço do gateway-py; `/swagger` redireciona) via `swaggo/swag` + `gin-swagger`; spec gerado em `docs/` com `make swagger`, que usa `go tool swag` (versão fixada no `go.mod`; um `go mod tidy` removia a dependência quando era só `go run`).
- Validação de payload com JSON Schema 2020-12 sobre `contracts/jobs`, os mesmos arquivos do gateway-py.
- Testes: validação contra os schemas reais, status derivado, envelope contra `envelope.schema.json`, códigos HTTP, Swagger servido e repositório contra o Postgres (atomicidade da outbox, resultados).
- `Dockerfile` (multi-stage, distroless), serviço `gateway-go` no compose (profile `go`, porta 8001); targets `run-go` e `swagger`; `gofmt` e `test` incluem o gateway-go.

#### Verificado
- Job criado pelo gateway-go: outbox → relay → worker-asyncio → `completed`, e aparece em `GET /jobs` do gateway-py.

### 2026-10-07 · Fase 1: worker-asyncio (fecha o caminho Python)

#### Adicionado
- `services/worker-asyncio` (aio-pika, asyncpg): consome `jobs.asyncio`, valida envelope e payload contra `contracts/`, executa o handler e grava em `job_results`. Primeiro handler: `io.sleep` com `asyncio.sleep`.
- Ack explícito só após gravar o resultado; mensagem inválida vai para a DLQ; falha de infra tem uma segunda chance e depois vai para a DLQ; gravação idempotente por `(job_id, worker)`.
- Falha de handler vira resultado `failed` (não reentrega).
- Métricas `worker_jobs_processed_total` e `worker_job_duration_seconds` em `:9101/metrics`.
- Testes: contrato, processor (sucesso, falha, duplicata, inválida), decisão de ack/reject e repositório contra o Postgres (pula sem banco).
- `Dockerfile`, serviço `worker-asyncio` no compose (profiles `python`, `all`); `make ruff` e `make test` incluem o worker.

#### Verificado
- Critério da fase 1: com o RabbitMQ parado, `POST /jobs` responde `202`; ao voltar, o relay publica e o job completa.
- 100 jobs `io.sleep` de 1 s terminam em poucos segundos (concorrência do event loop com prefetch 64).

### 2026-10-07 · Fase 1: outbox-relay (Go)

#### Adicionado
- `services/outbox-relay` (Go, `pgx`, `amqp091-go`, `slog`, Prometheus): lê a `outbox` com `FOR UPDATE SKIP LOCKED`, publica com publisher confirms e só então marca `published_at` e commita (at-least-once). Roteia `routing_key` vazia para o fanout `jobs` e as demais para `jobs.direct`.
- Publicação com `mandatory=true`: mensagem sem rota é devolvida pelo broker e não conta como publicada, evitando perda silenciosa.
- Purga periódica de linhas publicadas e métricas `outbox_pending`, `outbox_publish_lag_seconds`, `outbox_published_total`, `outbox_publish_errors_total` em `:9100/metrics`.
- Testes: config, laço do relay com fakes, store contra o Postgres (publicação parcial, sem confirm, SKIP LOCKED, purga) e publisher contra o RabbitMQ (entrega intacta, sem rota). Integração pula sem infra.
- `Dockerfile` (multi-stage, distroless), serviço `outbox-relay` no compose (profiles `python`, `go`, `all`) e target `make gofmt`; `make test` passa a rodar também os testes Go.

#### Corrigido
- `definitions.json` do RabbitMQ não definia usuários, então, ao carregar as definições, o `guest` deixava de existir e nenhum serviço conseguia conectar (403). Usuário e permissões agora fazem parte das definições.

#### Alterado
- README: relay descrito como implementado; o envelope é publicado sem reinterpretar, mas não idêntico byte a byte, pois o `jsonb` normaliza o texto.

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
