# Decisões (ADRs curtos)

Cada entrada registra uma decisão com trade-off: o que foi escolhido, o que foi descartado e por quê. O README traz o contexto; aqui fica o raciocínio resumido.

## Bridge para Celery e TaskIQ

**Decisão:** o envelope neutro continua único. Um consumidor fino (bridge) lê `jobs.<stack>`, valida o contrato e entrega a task ao framework, que roda num processo separado (`celery-bridge` + `worker-celery`, `taskiq-bridge` + `worker-taskiq`, uma imagem por stack).

**Descartado:** o gateway publicar no formato nativo de cada framework. Acoplaria o Go (e o relay) ao Celery e ao TaskIQ, e o contrato deixaria de ser único.

**Custo:** um hop a mais, que entra na latência medida dos dois frameworks. O benchmark registra isso.

**Consequência no ack:** nos workers asyncio e Go o `ack` vem depois do resultado gravado. Na bridge o `ack` é de entrega: confirma a mensagem original depois de o broker aceitar a task do framework (`confirm_publish` no Celery, publish aguardado no TaskIQ). O resultado é gravado depois pela task, com ack tardio (`task_acks_late`) e gravação idempotente por `(job_id, worker)`, então nenhuma etapa perde o job sem o broker saber.

**Validação dupla:** a bridge valida para mandar mensagem inválida à DLQ antes de gastar uma task; a task valida de novo porque é um ponto de entrada próprio do framework.

**Código duplicado de propósito:** `contracts.py`, `models.py` e o repositório se repetem entre os workers Python. A regra do projeto proíbe dependência cruzada entre serviços além de `contracts/`, e cada worker precisa ser construído e medido isolado.

## Jobs determinísticos e `io.fetch_urls` tolerante a status

**Decisão:** o resultado de cada job precisa ser idêntico nas quatro stacks, e `contracts/jobs/examples.json` é o árbitro (todos os workers testam contra ele). Para isso, `data.json_transform` gera registros com um LCG de 64 bits próprio, e não com `random`, e `cpu.pbkdf2` fixa o algoritmo e o tamanho do digest.

**Descartado:** receber 1-5 MB no payload do `data.json_transform`. O envelope passa pelo Postgres (`jsonb`) e pelo RabbitMQ, então o job mediria o transporte e não a (de)serialização do worker. Gerar a partir de `seed` isola o que o job quer medir.

**`io.fetch_urls`:** status 4xx/5xx é resultado (`status` no JSON); erro de rede ou timeout falha o job inteiro. Resultado parcial dependeria da ordem de chegada e não seria comparável entre stacks. Sem redirects e com timeout de 10 s em todas.

**Custo:** o gerador e a agregação existem duas vezes (Python e Go). Os vetores dourados pegam qualquer divergência.

## Observabilidade: traces por OTLP, métricas por scrape e Postgres

**Decisão:** traces vão por OTLP/HTTP ao `otel-collector` e daí ao Jaeger. Métricas de infra (fila, outbox, CPU, memória) são scrape do Prometheus. Throughput e latência por worker vêm de `job_results` no Postgres, consultado direto pelo Grafana.

**Descartado:** métricas de worker via OTLP/`prometheus_client`. Celery (prefork) e TaskIQ (multiprocess) exigiriam modo multiprocess e agregação para expor números comparáveis, e o benchmark mediria a instrumentação. `job_results` já tem `started_at`/`finished_at` gravados da mesma forma pelas 4 stacks.

**Relay e worker irmãos no trace:** o envelope publicado é byte a byte o que o gateway gravou, incluindo o `traceparent` do span `enqueue job`. Fazer o relay reescrevê-lo para ficar entre gateway e worker quebraria a regra "envelope intacto". Custo: o trace não mostra o relay como pai do worker; o tempo na fila aparece como lacuna entre `publish outbox` e `process`.

**Bridges sem span e sem spans de SQL:** a bridge só repassa e a task continua o trace pelo envelope; spans de SQL somariam ruído e overhead aos números comparados.

**Provider não global, um por dono:** cada processo cria e descarrega o próprio `TracerProvider`. No Celery (prefork) é um por processo filho, porque o provider (e suas threads de exportação) não sobrevive ao fork.
