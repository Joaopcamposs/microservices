# Microservices Lab: Python vs Go na prática

Projeto de estudo, sem fins comerciais, para comparar Python (FastAPI, Celery, TaskIQ, AsyncIO) e Go (Gin, goroutines) em um sistema de microsserviços com mensageria.

---

## 1. Propósito

### Perguntas que o projeto responde

1. Em quais cenários Go realmente ganha de Python (CPU-bound, memória, startup) e em quais a diferença é pequena (I/O-bound)?
2. O Python 3.14 free-threaded fecha a lacuna em CPU-bound?
3. Como Celery, TaskIQ e AsyncIO puro se comportam sob a mesma carga?
4. O quanto custa, em tempo de desenvolvimento, escrever o mesmo serviço em Go?
5. Como orquestrar um sistema poliglota (Python e Go) com um contrato único entre serviços?

### Resultado esperado

- Um sistema funcionando de ponta a ponta com 2 gateways, 1 relay de outbox e 4 workers.
- Um relatório de benchmark com números seus, não opinião de internet.
- Experiência prática em Go, com goroutines, channels e tratamento explícito de erros.
- Uma arquitetura poliglota documentada, com observabilidade distribuída e decisões registradas.

### Fora de escopo (por enquanto)

CI/CD, Kubernetes, autenticação e frontend. Ficam como fases opcionais no final.

---

## 2. Arquitetura

```
              ┌────────────┐      ┌────────────┐
   cliente ──►│ gateway-py │      │ gateway-go │◄── cliente
              │  (FastAPI) │      │   (Gin)    │
              └─────┬──────┘      └─────┬──────┘
                    │ 1 transação:      │
                    │ jobs + outbox     │
                    ▼                   ▼
              ┌──────────────────────────────┐
              │ Postgres: tabela "outbox"    │
              └──────────────┬───────────────┘
                             │ poll (SKIP LOCKED)
                             ▼
                    ┌─────────────────┐
                    │  outbox-relay   │  publica com confirm
                    └────────┬────────┘
                             ▼
              ┌──────────────────────────────┐
              │  RabbitMQ: exchange "jobs"   │
              │  (fanout, 1 fila por worker) │
              └──┬──────┬──────┬──────┬──────┘
                 ▼      ▼      ▼      ▼
            ┌───────┐┌───────┐┌───────┐┌───────┐
            │celery ││taskiq ││asyncio││  go   │
            └───┬───┘└───┬───┘└───┬───┘└───┬───┘
                └────────┴───┬────┴────────┘
                             ▼
                   ┌──────────────────┐
                   │ Postgres (status)│
                   └──────────────────┘

   Todos os serviços ──► OpenTelemetry Collector ──► Grafana/Tempo/Prometheus
```

### Por que outbox?

Publicar o job tem um problema de escrita dupla: o gateway grava o status no Postgres **e** publica no RabbitMQ. Os dois não participam da mesma transação, então qualquer ordem falha:

- Publica e depois grava: se o banco cair, o worker processa um job que o `GET /jobs/{id}` desconhece.
- Grava e depois publica: se o broker cair ou o processo morrer entre os dois passos, o job fica `PENDING` para sempre e nunca é processado.

O padrão **transactional outbox** elimina a janela. O gateway não fala com o RabbitMQ. Em uma única transação ele insere o job em `jobs` e o envelope em `outbox`. Um processo separado, o `outbox-relay`, lê as linhas pendentes e publica no broker. Ou as duas escritas acontecem, ou nenhuma.

Consequências:

- **Garantia at-least-once na publicação:** se o relay publicar e cair antes de marcar a linha como enviada, ele republica depois. O mesmo `job_id` pode chegar duas vezes, e os workers já precisam ser idempotentes (chave `(job_id, worker)`, cenário 7.2).
- **Um hop a mais de latência:** o intervalo de polling do relay entra na latência ponta a ponta. Registre-o separado no benchmark.
- **O broker fora do ar não derruba o `POST /jobs`:** o gateway continua respondendo `202` e a outbox acumula até o RabbitMQ voltar.
- **Um único relay para os dois gateways:** a lógica de publicação não é duplicada em Python e Go, e os gateways ficam equivalentes por construção.

### Topologia do RabbitMQ

Declarada em `infra/rabbitmq/definitions.json` e carregada no boot do broker, então nenhum serviço precisa criar filas:

| Item | Tipo | Função |
|---|---|---|
| `jobs` | exchange fanout | `target=all`: entrega o job às 4 filas |
| `jobs.direct` | exchange direct | `target=<stack>`: routing key = nome da stack |
| `jobs.celery`, `jobs.taskiq`, `jobs.asyncio`, `jobs.go` | filas duráveis | uma por worker, ligadas aos dois exchanges |
| `jobs.dlx` → `jobs.dlq` | exchange fanout + fila | destino de mensagens rejeitadas (`nack` sem requeue) |

O relay publica em `jobs` quando `outbox.routing_key` é vazia e em `jobs.direct` quando preenchida.

### Por que fanout?

O mesmo job chega aos 4 workers ao mesmo tempo. Isso dá uma comparação direta e justa: mesma entrada, mesmo instante, quatro implementações.

Consequência importante: **o resultado precisa ser gravado por worker**. A tabela de resultados tem chave composta `(job_id, worker)`.

Para testes de vazão sustentada, use também o modo "competing consumers": um exchange `direct` com uma fila por stack, e você escolhe para qual stack mandar (`?target=go`). Assim cada job é processado uma vez e você mede throughput real sem multiplicar a carga por 4.

---

## 3. Estrutura do repositório (monorepo)

```
microservices-lab/
├── services/
│   ├── gateway-py/        # FastAPI, pyproject.toml próprio
│   ├── gateway-go/        # Gin, go.mod próprio
│   ├── outbox-relay/      # lê a outbox e publica no RabbitMQ
│   ├── worker-celery/     # Celery + bridge
│   ├── worker-taskiq/     # TaskIQ + bridge
│   ├── worker-asyncio/    # aio-pika puro
│   └── worker-go/         # goroutines + worker pool
├── contracts/
│   ├── envelope.schema.json
│   └── jobs/              # schema do payload de cada tipo de job
├── db/
│   └── migrations/        # jobs, job_results e outbox (SQL puro, compartilhado)
├── infra/
│   ├── rabbitmq/          # definitions.json: exchanges, filas, bindings e DLQ
│   ├── otel-collector.yaml
│   ├── grafana/
│   └── prometheus.yml
├── bench/
│   ├── k6/                # scripts de carga
│   └── results/           # CSVs e gráficos por rodada
├── docs/
│   └── decisoes.md        # ADRs curtos
├── docker-compose.yml
├── Makefile
└── README.md
```

### Por que monorepo?

- Um único `docker compose up` sobe tudo.
- O contrato (`contracts/`) é compartilhado e versionado junto com os serviços.
- Mudança de contrato e de consumidores entra no mesmo commit.
- Cada serviço continua isolado: dependências, Dockerfile e build próprios. Python usa `pyproject.toml` (com `uv`) e Go usa `go.mod`.

Para 7 serviços de estudo, polyrepo seria só overhead.

---

## 4. O contrato do job (decisão mais importante)

Celery e TaskIQ têm formatos de mensagem próprios. Se o gateway Go publicar um JSON qualquer, o worker Celery não vai entender. A solução é um **envelope neutro**:

```json
{
  "job_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "type": "cpu.pbkdf2",
  "payload": { "password": "abc", "iterations": 600000, "rounds": 50 },
  "created_at": "2026-10-07T04:30:00Z",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "attempt": 0,
  "origin": "gateway-go"
}
```

| Campo | Para quê |
|---|---|
| `job_id` | Idempotência e correlação |
| `type` | Roteia para a função certa em cada worker |
| `payload` | Entrada específica do job (validada por JSON Schema) |
| `created_at` | Mede latência ponta a ponta |
| `traceparent` | Propaga o trace do OpenTelemetry entre linguagens |
| `attempt` | Controle de retry |
| `origin` | Qual gateway recebeu o pedido |

### Celery e TaskIQ: o padrão "bridge"

Os dois frameworks esperam o formato deles. Em cada um, um consumidor fino (bridge) lê o envelope da fila `jobs.<stack>` e chama a task do framework:

```python
# worker-celery/bridge.py (Python 3.11+)
import asyncio
import json

import aio_pika

from app.tasks import process_job  # task Celery


async def main() -> None:
    conn = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=32)
    queue = await channel.declare_queue("jobs.celery", durable=True)

    async with queue.iterator() as it:
        async for message in it:
            async with message.process(requeue=False):
                envelope = json.loads(message.body)
                process_job.delay(envelope)  # entrega ao Celery


if __name__ == "__main__":
    asyncio.run(main())
```

O envelope é gravado como JSON na coluna `outbox.envelope` e o relay o publica **sem reinterpretar** (o corpo da mensagem é o JSON da coluna; o `jsonb` normaliza espaços e ordem das chaves, então não é idêntico byte a byte ao que o gateway montou, só equivalente). Assim o contrato continua único e o relay não conhece os tipos de job.

Trade-off: é artificial (um hop a mais), mas mantém o contrato único. A alternativa, o gateway Go publicar no formato nativo de cada framework, acopla o Go ao Celery e não vale a pena. Documente o hop extra como parte do resultado ao comparar latência.

---

## 5. Serviços

### 5.1 gateway-py (FastAPI)

- `POST /jobs?type=...&target=all|celery|taskiq|asyncio|go`: valida, cria o envelope, grava `jobs` + `outbox` na mesma transação (sem falar com o RabbitMQ) e retorna `202` com `job_id`.
- `GET /jobs/{job_id}`: consulta os resultados por worker no Postgres.
- `GET /jobs?limit=20`: jobs mais recentes, para achar `job_id`s.
- `GET /outbox?state=pending|published|all`: inspeciona a outbox (aba `debug`), para conferir se o job saiu do gateway e se o relay está esvaziando.
- `GET /healthz` (verifica o Postgres) e `/metrics` (Prometheus).

**Swagger:** `http://localhost:8000/docs` é a interface de teste. Os endpoints têm descrição, tags e exemplo de payload pronto para o botão "Try it out".

O corpo do `POST` é o `payload` do job (JSON), validado contra `contracts/jobs/<type>.schema.json`. Tipo sem schema ou payload inválido retorna `422`. O `GET` devolve `status` agregado: `pending` (nenhum resultado), `running` (parcial) ou `completed` (4 resultados no `target=all`, 1 nos demais).

Código em `services/gateway-py/app`: `api/` (rotas), `services/` (caso de uso e `Services`, dono do estado de processo criado no `lifespan`), `domain/` (envelope, validador de payload) e `infra/` (repositório Postgres). O `traceparent` é gerado localmente por enquanto; passa a vir do span OpenTelemetry na fase 5.

Uso local (com `make up` rodando):

```bash
make run     # uvicorn com reload em :8000
curl -XPOST 'localhost:8000/jobs?type=io.sleep&target=go' -H 'content-type: application/json' -d '{"ms": 100}'
curl localhost:8000/jobs/<job_id>
make test    # inclui integração com o Postgres; pula se o banco estiver fora
```

### 5.2 gateway-go (Gin)

Mesma API, mesmo contrato, mesma transação `jobs` + `outbox`, comportamento idêntico. Esse é o teste de paridade: os dois gateways precisam ser intercambiáveis.

### 5.2.1 outbox-relay

Serviço único, compartilhado pelos dois gateways. Seu trabalho é esvaziar a tabela `outbox` para o RabbitMQ.

Schema (`db/migrations`):

```sql
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    job_id       UUID        NOT NULL,
    routing_key  TEXT        NOT NULL,          -- "" para fanout, stack para direct
    envelope     JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- índice parcial: só as linhas pendentes, que é o que o relay consulta
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
```

Escrita no gateway (uma transação, sem I/O de broker):

```sql
BEGIN;
INSERT INTO jobs (job_id, type, created_at) VALUES ($1, $2, now());
INSERT INTO outbox (job_id, routing_key, envelope) VALUES ($1, $3, $4);
COMMIT;
```

Laço do relay:

1. Abre transação e seleciona um lote: `SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`.
2. Publica cada linha no exchange `jobs` com **publisher confirms** ligados e `delivery_mode=2` (persistente).
3. Espera os confirms; só então executa `UPDATE outbox SET published_at = now() WHERE id = ANY($1)` e faz `COMMIT`.
4. Sem linhas pendentes, dorme o intervalo de polling (padrão 50 ms, configurável).

Pontos que importam:

- `FOR UPDATE SKIP LOCKED` permite rodar várias réplicas do relay sem processar a mesma linha duas vezes ao mesmo tempo.
- O `COMMIT` vem **depois** do confirm do broker. Se o relay cair no meio, a transação reverte e as linhas voltam a ser pendentes (at-least-once, nunca at-most-once).
- `ORDER BY id` preserva a ordem de inserção por relay, mas não há garantia de ordem global com múltiplas réplicas. Os jobs são independentes, então isso não importa aqui.
- Limpeza: um job periódico apaga linhas com `published_at` antigo (ex.: > 1 h) para a tabela não crescer sem limite.
- Métricas: `outbox_pending` (gauge), `outbox_publish_lag_seconds` (histograma de `published_at - created_at`) e `outbox_publish_errors_total`.

Implementação em `services/outbox-relay` (Go, `pgx` + `amqp091-go`), em pacotes com dependências invertidas: `outbox` (laço, interfaces `Store`/`Publisher`), `postgres` (claim/mark/purge), `rabbitmq` (publisher com confirm), `config`, `metrics`.

- **Mensagem sem rota é falha:** o publish usa `mandatory=true`; se nenhuma fila recebe a mensagem (ex.: `routing_key` desconhecida), o broker a devolve e o relay **não** a marca como publicada. Sem isso, o ack do broker esconderia a perda do job.
- **Publicação parcial:** só os IDs confirmados são marcados e commitados; o resto continua pendente e é reenviado (duplicata possível, daí a idempotência dos workers).
- **Reconexão:** o relay conecta ao RabbitMQ sob demanda e reconecta sozinho; broker fora do ar vira erro contado em `outbox_publish_errors_total`, com 1 s de espera entre tentativas.
- **Configuração** (env): `RELAY_DATABASE_URL`, `RELAY_AMQP_URL`, `RELAY_POLL_INTERVAL` (50ms), `RELAY_BATCH_SIZE` (100, máx. 1000), `RELAY_RETENTION` (1h), `RELAY_PURGE_INTERVAL` (1m), `RELAY_METRICS_ADDR` (:9100).
- **Observação:** `GET :9100/metrics` e `GET :9100/healthz`. Também `outbox_published_total`.

```bash
docker compose --profile python up -d --build outbox-relay   # sobe o relay
curl localhost:9100/metrics | grep outbox_                   # pendentes, lag, erros
cd services/outbox-relay && go test ./...                    # integração pula sem infra
```

Alternativa descartada: CDC com Debezium lendo o WAL. É mais robusto em escala, mas adiciona Kafka Connect/Debezium à infra, o que foge do foco do estudo. O polling é suficiente e mais simples de entender e medir. Fica como extensão opcional.

### 5.3 worker-asyncio (aio-pika puro)

```python
# worker-asyncio/main.py (Python 3.11+)
import asyncio
import json
from datetime import UTC, datetime

import aio_pika

from handlers import HANDLERS  # dict[str, Callable]
from db import save_result


async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    async with message.process(requeue=False):
        envelope = json.loads(message.body)
        started = datetime.now(UTC)
        result = await HANDLERS[envelope["type"]](envelope["payload"])
        await save_result(
            job_id=envelope["job_id"],
            worker="asyncio",
            started_at=started,
            finished_at=datetime.now(UTC),
            result=result,
        )


async def main() -> None:
    conn = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=64)
    queue = await channel.declare_queue("jobs.asyncio", durable=True)
    await queue.consume(handle)
    await asyncio.Future()  # roda para sempre


if __name__ == "__main__":
    asyncio.run(main())
```

Handlers CPU-bound devem usar `asyncio.to_thread` ou `ProcessPoolExecutor`, senão bloqueiam o event loop. Isso em si é uma lição do experimento.

### 5.3.1 worker-celery

Celery com pool `prefork`, `-c 4`. Para I/O-bound, teste também `--pool=gevent` ou `threads` e registre a diferença.

### 5.3.2 worker-taskiq

TaskIQ é async nativo. Use `taskiq worker app:broker --workers 2`.

### 5.4 worker-go (goroutines)

```go
// worker-go/main.go
package main

import (
	"encoding/json"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Envelope struct {
	JobID   string          `json:"job_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func worker(id int, deliveries <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	for d := range deliveries {
		var env Envelope
		if err := json.Unmarshal(d.Body, &env); err != nil {
			d.Nack(false, false) // sem requeue
			continue
		}
		if err := handle(env); err != nil {
			d.Nack(false, false)
			continue
		}
		d.Ack(false)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, _ := conn.Channel()
	ch.Qos(64, 0, false) // prefetch
	deliveries, _ := ch.Consume("jobs.go", "", false, false, false, false, nil)

	const poolSize = 8
	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go worker(i, deliveries, &wg)
	}
	wg.Wait()
}
```

Conceitos de Go que você pratica aqui: goroutines, channels, `sync.WaitGroup`, `context` para graceful shutdown, tratamento explícito de erros e struct tags.

---

## 6. Jobs (workloads)

| Job | Tipo | O que faz | O que mede |
|---|---|---|---|
| `cpu.pbkdf2` | CPU-bound | PBKDF2/hash repetido N vezes | Paralelismo real, GIL, free-threading |
| `io.fetch_urls` | I/O-bound | Busca N URLs de um mock server local | Concorrência de I/O |
| `io.sleep` | I/O-bound puro | `sleep` de X ms | Overhead do framework/scheduler |
| `data.json_transform` | Serialização | Parse, transformação e serialização de JSON de 1-5 MB | Custo de (de)serialização |
| `pipeline.fanout` | Orquestração | Divide em N subjobs e agrega | Coordenação e fan-in |

**Dica:** use um mock server local (um container com respostas de latência controlada) para o `io.fetch_urls`. Internet real torna o benchmark irreproduzível.

---

## 7. Cenários extras de teste

Além de CPU-bound e I/O-bound, estes cenários revelam diferenças que benchmark de velocidade pura não mostra.

### 7.1 Pico de carga e backpressure
Envie 10.000 jobs em 5 segundos e meça: crescimento da fila, tempo para drenar, uso de memória durante o pico, e se algum worker cai. Mostra como cada stack lida com contrapressão (`prefetch`, tamanho de pool).

### 7.2 Falhas, retries e idempotência
- Mate um worker no meio de um job (`docker kill`) e veja se a mensagem é reentregue.
- Envie uma mensagem "veneno" (payload inválido) e confirme que ela vai para uma DLQ sem travar a fila.
- Entregue o mesmo `job_id` duas vezes e confirme que o resultado não duplica.
- **Outbox:** derrube o RabbitMQ (`docker compose stop rabbitmq`), envie jobs e confirme que o gateway responde `202`, a `outbox` acumula e tudo é entregue quando o broker volta, sem job perdido.
- **Outbox:** mate o `outbox-relay` entre o publish e o `COMMIT` e confirme que as linhas são republicadas e os workers descartam o duplicado pela chave `(job_id, worker)`.

Esse cenário ensina semântica *at-least-once* e é um dos cenários mais didáticos do projeto.

### 7.3 Footprint: startup, memória ociosa e tamanho de imagem
Por serviço, registre: tamanho da imagem, tempo até ficar pronto, RSS em idle, RSS sob carga. É onde a diferença estrutural entre Python e Go costuma ser mais visível, mesmo quando o throughput empata.

### 7.4 Python 3.14 free-threaded vs padrão
Rode o `worker-asyncio` (com `ThreadPoolExecutor` nos handlers CPU-bound) em duas builds: CPython 3.14 padrão e 3.14 free-threaded (`python3.14t`). Compare com o worker Go. Responde diretamente à dúvida sobre o fim do GIL. Verifique a disponibilidade da imagem Docker da build free-threaded; se não houver, instale com `uv python install 3.14t`. Também teste a compatibilidade das suas dependências.

### 7.5 Graceful shutdown e escala horizontal
- Envie `SIGTERM` durante a carga: o worker termina os jobs em andamento ou perde mensagens?
- Escale cada worker para 1, 2, 4 réplicas (`docker compose up --scale`) e plote o throughput. Mostra a eficiência de escala por stack.

### 7.6 Jobs agendados
Celery Beat vs scheduler do TaskIQ vs `time.Ticker` em Go, executando um job a cada minuto. Compara ergonomia e confiabilidade.

### 7.7 Backlog persistente
Pare todos os workers, enfileire 50.000 jobs, ligue os workers e meça o tempo de drenagem de cada stack separadamente (modo competing consumers).

### 7.8 Bônus opcional: worker em Rust
Se sobrar energia, um `worker-rust` com `lapin` + `tokio` fecha o triângulo. Prioridade baixa: é a linguagem com a maior curva de aprendizado.

---

## 8. Observabilidade

Você já domina OpenTelemetry, então esta fase é para ver o trace atravessando as linguagens.

- Cada serviço exporta traces e métricas via OTLP para o `otel-collector`.
- O `traceparent` vai dentro do envelope: o gateway o grava na outbox junto com o resto, o relay o preserva ao publicar, e o worker extrai e continua o trace. Um único trace mostra `gateway → outbox → relay → fila → worker → banco`.
- Dashboards no Grafana: throughput, latência p50/p95/p99, tamanho da fila, `outbox_pending` e `outbox_publish_lag_seconds`, CPU e memória por container (cAdvisor).

---

## 9. Metodologia de benchmark (para o resultado ser justo)

1. **Mesmos recursos:** `deploy.resources.limits` iguais em todos os containers (ex.: 1 CPU, 512 MB).
2. **Mesma configuração lógica:** prefetch e concorrência comparáveis; registre o que usou, incluindo o intervalo de polling e o tamanho de lote do relay (iguais para todas as stacks, já que o relay é único).
3. **Warm-up:** descarte os primeiros 30 s.
4. **Repetição:** pelo menos 5 rodadas por cenário; reporte mediana e dispersão.
5. **Máquina limpa:** nada pesado rodando em paralelo.
6. **Ferramenta de carga:** k6 (ou `hey`) contra os gateways.
7. **Registre tudo** em `bench/results/` (CSV + gráfico + commit hash).

### Métricas

| Categoria | Métrica |
|---|---|
| Vazão | jobs/s processados |
| Latência | p50 / p95 / p99 ponta a ponta, com o lag da outbox (`published_at - created_at`) separado |
| Recursos | CPU%, RSS (MB), por container |
| Footprint | tamanho da imagem, startup, RSS idle |
| Confiabilidade | jobs perdidos, duplicados, tempo de recuperação |
| Esforço | linhas de código e horas gastas por worker |

A última linha importa: o custo de desenvolvimento faz parte da resposta.

---

## 10. docker-compose (esqueleto)

Hoje o `docker-compose.yml` do repositório tem `rabbitmq`, `postgres` e `gateway-py` (profile `python`); os demais serviços entram conforme cada fase. O build do gateway usa a raiz do repo como contexto para copiar `contracts/` para a imagem. O Postgres aplica `db/migrations/*.sql` na primeira inicialização. Para recriar o banco do zero, use `make reset`.

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports: ["5672:5672", "15672:15672"]
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s

  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: lab
      POSTGRES_DB: lab
    ports: ["5432:5432"]
    volumes: ["./db/migrations:/docker-entrypoint-initdb.d:ro"]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 10s

  outbox-relay:
    build: ./services/outbox-relay     # multi-stage: golang -> distroless
    depends_on:
      rabbitmq: { condition: service_healthy }
      postgres: { condition: service_healthy }
    profiles: [python, go, all]        # sempre presente: os dois gateways dependem dele

  gateway-py:
    build: { context: ., dockerfile: services/gateway-py/Dockerfile }   # python:3.13-slim
    ports: ["8000:8000"]
    depends_on: { postgres: { condition: service_healthy } }
    profiles: [python, all]

  gateway-go:
    build: ./services/gateway-go       # multi-stage: golang -> distroless
    ports: ["8080:8080"]
    depends_on: { postgres: { condition: service_healthy } }
    profiles: [go, all]

  worker-celery:
    build: ./services/worker-celery
    command: celery -A app worker -c 4
    deploy: { resources: { limits: { cpus: "1", memory: 512M } } }
    depends_on: { rabbitmq: { condition: service_healthy } }
    profiles: [python, all]

  worker-taskiq:
    build: ./services/worker-taskiq
    command: taskiq worker app:broker --workers 2
    deploy: { resources: { limits: { cpus: "1", memory: 512M } } }
    depends_on: { rabbitmq: { condition: service_healthy } }
    profiles: [python, all]

  worker-asyncio:
    build: ./services/worker-asyncio
    deploy: { resources: { limits: { cpus: "1", memory: 512M } } }
    depends_on: { rabbitmq: { condition: service_healthy } }
    profiles: [python, all]

  worker-go:
    build: ./services/worker-go
    deploy: { resources: { limits: { cpus: "1", memory: 512M } } }
    depends_on: { rabbitmq: { condition: service_healthy } }
    profiles: [go, all]
```

Comandos úteis:

```bash
docker compose --profile all up --build      # tudo
docker compose --profile go up --build       # só o lado Go
docker compose up --build gateway-go outbox-relay worker-go rabbitmq postgres
```

---

## 11. Fases de implementação

| Fase | Entrega | Critério de pronto |
|---|---|---|
| 0. Fundação | Repo, compose com RabbitMQ e Postgres, `envelope.schema.json`, migrations (`jobs`, `job_results`, `outbox`), Makefile | `make up` sobe a infra e aplica as migrations |
| 1. Caminho Python | gateway-py + outbox-relay + worker-asyncio + job `io.sleep` | `POST /jobs` → linha na outbox → publicada → resultado no `GET`; com o broker parado, o `POST` ainda responde `202` e o job é entregue depois |
| 2. Caminho Go | gateway-go (reaproveita o relay) + worker-go; aprenda Gin, `amqp091-go`, goroutines | Mesmo teste da fase 1 passa |
| 3. Celery e TaskIQ | Dois workers com bridge | Os 4 workers respondem ao mesmo job |
| 4. Workloads | `cpu.pbkdf2`, `io.fetch_urls`, `data.json_transform` implementados nos 4 workers | Resultados idênticos entre stacks |
| 5. Observabilidade | OTel, Grafana, trace atravessando as linguagens | Um trace mostra gateway → worker |
| 6. Benchmark base | CPU-bound, I/O-bound, footprint | Relatório v1 no README |
| 7. Cenários extras | Seção 7 (pico, falhas, free-threading, escala) | Relatório v2 |
| 8. Opcional | Kubernetes (Helm/manifests), CI com GitHub Actions, worker Rust | À vontade |

### Ordem sugerida por semana (6-10 h/semana)

- Semana 1: fases 0 e 1.
- Semana 2: fase 2 (a mais densa em aprendizado de Go).
- Semana 3: fases 3 e 4.
- Semana 4: fases 5 e 6.
- Semana 5 em diante: fase 7, no ritmo que preferir.

---

## 12. Riscos e armadilhas

- **Benchmark injusto:** configurações diferentes (prefetch, concorrência) invalidam a comparação. Documente tudo.
- **Event loop bloqueado:** CPU-bound dentro de `async def` derruba o AsyncIO. Use `to_thread` ou processos.
- **Outbox cresce sem limite:** sem limpeza das linhas publicadas, a tabela e o índice incham e o polling degrada. Agende a purga e monitore `outbox_pending`.
- **Relay como gargalo:** polling lento ou lote pequeno limita a vazão e contamina a latência medida. Ajuste e registre os mesmos parâmetros em todas as rodadas.
- **Duplicatas após falha do relay:** são esperadas (at-least-once). A idempotência nos workers não é opcional.
- **Fanout multiplica a carga por 4:** para throughput, use o modo competing consumers.
- **Celery com `acks_late`:** entenda a semântica antes de medir falhas.
- **Escopo:** é um projeto de estudo. Termine a fase 6 antes de começar a decorar com extras.

---

## 13. Como documentar o projeto

- README com diagrama, tabela de resultados e uma conclusão honesta: "Go ganhou em X, empatou em Y, perdeu em esforço Z".
- `docs/decisoes.md` com ADRs curtos (por que fanout, por que bridge, por que monorepo, por que outbox com polling e não CDC).
- Dashboards do Grafana em prints.
- Um resumo curto dos números principais, com a metodologia usada para chegar neles.

---

## 14. Stack resumida

| Camada | Python | Go |
|---|---|---|
| Gateway | FastAPI + Pydantic v2 | Gin |
| Workers | Celery, TaskIQ, aio-pika | `amqp091-go` + goroutines |
| Mensageria | RabbitMQ | RabbitMQ |
| Publicação confiável | Transactional outbox (tabela `outbox`) | `outbox-relay` em Go (`pgx` + `amqp091-go`) |
| Banco | Postgres (SQLAlchemy async) | Postgres (`pgx`) |
| Observabilidade | OpenTelemetry SDK | OpenTelemetry SDK |
| Carga | k6 | k6 |
| Orquestração | Docker Compose | Docker Compose |
