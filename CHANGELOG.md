# Changelog

Registro da evolução do código, no formato [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/). Um único arquivo, com a entrada mais recente no topo. Cada entrada usa a data e a fase do plano (seção 11 do `README.md`) e agrupa por `Adicionado`, `Alterado`, `Corrigido` e `Removido`. Escreva o **porquê** quando a decisão não for óbvia.

## [Não lançado]

### 2026-10-07 · Fase 6: benchmark base

#### Adicionado
- `bench/` (Python, uv próprio): driver `lab_bench` com os comandos `run`, `gateway` e `footprint`. Cenários `overhead` (`io.sleep` 0 ms), `cpu` (`cpu.pbkdf2`), `io` (`io.fetch_urls`, 5 GETs de 200 ms) e `serialization` (`data.json_transform`), 5 rodadas por worker, CSV bruto e resumo Markdown (mediana e dispersão) em `bench/results/`. Testes da estatística e do parse de `docker stats`.
- Alvos do Makefile: `bench-up`, `bench`, `bench-gateway`, `bench-footprint`; `make ruff` e `make test` incluem `bench/`.
- Limite de 1 CPU e 512 MB em todos os serviços de aplicação no compose (metodologia, item 1).
- `OTEL_ENDPOINT` no compose: vazio desliga a exportação de traces, para o benchmark não medir a instrumentação.
- Relatório v1 no README (seção 9): resultados, leitura e limitações.

#### Decisões
- Driver próprio em vez de k6/`hey`: a rodada precisa pausar o worker, esperar a outbox esvaziar e ler `job_results`. Ver `docs/decisoes.md`.
- Vazão medida drenando backlog (worker pausado durante o envio). A primeira versão media do primeiro POST ao último resultado e dava ~150 jobs/s em todas as stacks: era o limite do gateway, não do worker.

#### Verificado
- 80 rodadas (4 cenários x 4 workers x 5), ~34 mil jobs: 0 falhas, 0 perdidos.
- Limitação: o hash gravado no nome dos resultados é o do último commit; a árvore tinha mudanças da fase 6 sem commit.

### 2026-10-07 · Fase 5: observabilidade

#### Adicionado
- Tracing OpenTelemetry (OTLP/HTTP) em gateway-py, gateway-go, outbox-relay e nos quatro workers: spans `enqueue job` (gateway), `publish outbox` (relay) e `process <tipo>` com `run handler` e `save result` (worker). O worker continua o trace a partir do `traceparent` do envelope, atravessando Python e Go.
- Pilha no compose (perfil `observability`, também em `all`): `otel-collector`, `jaeger`, `prometheus`, `cadvisor` e `grafana`, com imagens de versão fixa. Configuração em `infra/otel-collector.yaml`, `infra/prometheus.yml` e `infra/grafana/` (datasources Prometheus, Jaeger e Postgres; dashboard `lab.json`).
- Dashboard "Microservices Lab": throughput, latência p50/p95/p99, latência ponta a ponta, fila do RabbitMQ, `outbox_pending`, `outbox_publish_lag_seconds`, CPU e memória por container.
- Testes: span filho do `traceparent` do envelope em relay, worker-go e workers Python; `traceparent` gravado pelo span em gateway-py e gateway-go.

#### Alterado
- O `traceparent` do envelope deixou de ser gerado por `domain/traceparent` (removido nos dois gateways) e passou a vir do span ativo.
- gateway-py: o `FastAPIInstrumentor` é aplicado em `create_app`, e o tracing nativo do FastAPI 0.142 é desligado (`telemetry={"tracing": False}`). Por quê: com `OTEL_EXPORTER_OTLP_ENDPOINT` o FastAPI cria um provider global próprio (`service.name` genérico) e duplicava o span HTTP; instrumentar no lifespan era tarde demais, pois o Starlette monta os middlewares antes. `Services` agora recebe o `TracerProvider`.
- `NewRelay`, `NewJobProcessor` e `NewJobService` (Go) e `JobService`/`JobProcessor` (Python) recebem um tracer injetado.

#### Decisões
- Throughput e latência por worker saem do Postgres (`job_results`), não de métricas OTLP: é uniforme entre as 4 stacks (Celery e TaskIQ são multiprocess). Ver `docs/decisoes.md`.
- Relay e worker são irmãos no trace: o relay não reescreve o envelope. Bridges sem span; sem spans de SQL.

#### Verificado
- Critério da fase 5: um job com `target=all` pelo gateway-py e pelo gateway-go gera um único trace no Jaeger com gateway, relay e os 4 workers.
- As 11 consultas do dashboard executam sem erro pela API do Grafana; os 9 alvos do Prometheus ficam `up`.
- Limitação: no Docker Desktop (macOS) o cAdvisor não expõe o label `name` dos containers, então os painéis de CPU/memória ficam vazios.

### 2026-10-07 · Fase 4: workloads nos 4 workers

#### Adicionado
- Schemas de payload `cpu.pbkdf2`, `io.fetch_urls` e `data.json_transform` em `contracts/jobs/`; os dois gateways os carregam sem mudança de código.
- `contracts/jobs/examples.json`: vetores dourados (`payload -> resultado`) que os quatro workers usam nos testes. Por quê: é a prova de paridade entre Python e Go, sem depender de rede.
- Handlers dos três tipos em worker-asyncio, worker-taskiq (async, CPU em `asyncio.to_thread`), worker-celery (síncrono, `ThreadPoolExecutor` no fetch) e worker-go (`crypto/pbkdf2`, uma goroutine por URL). Dependência `httpx` nos três workers Python.
- `data.json_transform` gera os registros com um LCG de 64 bits igual nas duas linguagens, e não com `random`, para o JSON intermediário (`input_bytes`) e o resultado serem idênticos.
- `services/mock-server` (Go, stdlib, porta 8090): `/delay/{ms}`, `/status/{code}` e `/healthz`, com teste; serviço no compose (profiles `python`, `go`, `all`); `make gofmt` e `make test` o incluem.
- Exemplos de payload dos novos tipos no Swagger do gateway-py (`openapi_examples`) e na descrição do `POST /jobs` do gateway-go.
- Testes de handler por worker: vetores dourados e `io.fetch_urls` contra servidor HTTP local (ordem, status, tamanho, erro de conexão).

#### Alterado
- gateway-go: o corpo do `POST /jobs` no Swagger virou `object` (o DTO `JobPayload` só descrevia `io.sleep` e foi removido); os exemplos por tipo estão na descrição. Swagger regenerado.
- Testes dos gateways que usavam `cpu.pbkdf2`/`io.fetch_urls` como "tipo sem schema" passaram a usar `pipeline.fanout`, o único sem schema agora.
- README: seção 6 com payloads, formato dos resultados, regras de paridade e o mock-server; estrutura do repositório e esqueleto do compose.

#### Decisões
- `io.fetch_urls` com erro de rede falha o job inteiro; status 4xx/5xx é resultado, não erro. Resultado parcial não seria comparável entre stacks.
- `data.json_transform` gera o JSON a partir de `seed` em vez de receber 1-5 MB no payload: um payload grande mediria o broker e o `jsonb`, não a serialização do worker.

#### Verificado
- Critério da fase 4: um job de cada tipo com `target=all`, criado pelo gateway-py e pelo gateway-go, retornou `succeeded` com **resultado idêntico** nos 4 workers.
- Payload inválido (`cpu.pbkdf2` sem `salt`/`iterations`) responde `422` com as violações.

### 2026-10-07 · Fase 3: worker-celery e worker-taskiq (bridge)

#### Adicionado
- `services/worker-celery`: bridge aio-pika (`jobs.celery`) + task Celery `jobs.process`. A bridge valida o contrato, entrega com `send_task` (`confirm_publish`) e só então dá `ack`; a task roda o `JobProcessor` e grava `job_results` (psycopg, pool por processo filho, idempotente). Prefork `-c 4`, `acks_late`, retry com backoff se o banco cair. Prefetch total 64 (`-c 4` x multiplicador 16).
- `services/worker-taskiq`: mesma estrutura, com `taskiq-aio-pika` (fila clássica `taskiq.jobs`), asyncpg, 2 processos x `qos` 32 = 64 em voo, `SimpleRetryMiddleware`. Pool e processor criados no evento de startup, no `state` do worker.
- Mensagem inválida ou tipo sem handler vai para a DLQ na bridge, antes de gastar uma task. Métricas `bridge_messages_total{outcome}` em `:9103` e `:9104`.
- Testes por serviço: contrato, processor (sucesso, falha, duplicata, inválida), decisão de ack/reject/requeue da bridge e repositório contra o Postgres (pula sem banco).
- `Dockerfile` por stack (uma imagem serve bridge e worker), quatro serviços no compose (profiles `python`, `all`); `make ruff` e `make test` incluem os dois.
- `docs/decisoes.md` com o ADR da bridge.

#### Alterado
- O ack da bridge é de **entrega**, não de resultado: confirma a mensagem original depois de o broker do framework aceitar a task. Quebra a regra "ack só após gravar" de propósito; o ack tardio do framework e a gravação idempotente fecham a lacuna (ver ADR).

#### Limitações conhecidas
- O worker prefork do Celery e o worker do TaskIQ ainda não expõem métricas (modo multiprocess entra com a observabilidade, fase 5). Esgotadas as tentativas de retry por banco fora, a task falha só no log; o cenário é tratado na fase 7.

#### Verificado
- Critério da fase 3: um `POST` com `target=all` gera resultado `succeeded` dos 4 workers (`asyncio`, `celery`, `go`, `taskiq`) no mesmo job. 50 jobs `all` concorrentes completaram nos quatro; mensagem inválida em `jobs.celery` e `jobs.taskiq` foi para a DLQ.

### 2026-10-07 · Fase 2 (caminho Go): worker-go

#### Adicionado
- `services/worker-go` (amqp091, pgx): consome `jobs.go`, valida envelope e payload contra `contracts/`, executa o handler (`io.sleep`) e grava em `job_results` com worker `go`. Mesmas regras de entrega do worker-asyncio: ack só após gravar, inválida -> DLQ, falha de infra com um requeue e depois DLQ, handler que falha vira `failed`, gravação idempotente por `(job_id, worker)`.
- Pool de goroutines lendo o mesmo canal de deliveries, com `WORKER_POOL_SIZE` igual ao prefetch (64) por padrão. Por quê: benchmark justo, já que o asyncio tem concorrência igual ao prefetch.
- Graceful shutdown: cancela o consumer e espera o pool; o handler usa `context.WithoutCancel` para não gravar `failed` por causa do encerramento. Reconexão automática ao broker.
- Métricas `worker_jobs_processed_total` e `worker_job_duration_seconds` em `:9102/metrics`, com os mesmos nomes do asyncio.
- Testes: config, handler (inclusive cancelamento), processor com fakes (sucesso, falha, duplicata, inválidas, erro de infra), regra `Decide` de ack/reject/requeue e repositório contra o Postgres (pula sem banco).
- `Dockerfile` (multi-stage, distroless), serviço `worker-go` no compose (profiles `go`, `all`, porta 9102); `make gofmt` e `make test` incluem o worker.

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
