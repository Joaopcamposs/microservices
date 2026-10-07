# Decisões (ADRs curtos)

Cada entrada registra uma decisão com trade-off: o que foi escolhido, o que foi descartado e por quê. O README traz o contexto; aqui fica o raciocínio resumido.

## Bridge para Celery e TaskIQ

**Decisão:** o envelope neutro continua único. Um consumidor fino (bridge) lê `jobs.<stack>`, valida o contrato e entrega a task ao framework, que roda num processo separado (`celery-bridge` + `worker-celery`, `taskiq-bridge` + `worker-taskiq`, uma imagem por stack).

**Descartado:** o gateway publicar no formato nativo de cada framework. Acoplaria o Go (e o relay) ao Celery e ao TaskIQ, e o contrato deixaria de ser único.

**Custo:** um hop a mais, que entra na latência medida dos dois frameworks. O benchmark registra isso.

**Consequência no ack:** nos workers asyncio e Go o `ack` vem depois do resultado gravado. Na bridge o `ack` é de entrega: confirma a mensagem original depois de o broker aceitar a task do framework (`confirm_publish` no Celery, publish aguardado no TaskIQ). O resultado é gravado depois pela task, com ack tardio (`task_acks_late`) e gravação idempotente por `(job_id, worker)`, então nenhuma etapa perde o job sem o broker saber.

**Validação dupla:** a bridge valida para mandar mensagem inválida à DLQ antes de gastar uma task; a task valida de novo porque é um ponto de entrada próprio do framework.

**Código duplicado de propósito:** `contracts.py`, `models.py` e o repositório se repetem entre os workers Python. A regra do projeto proíbe dependência cruzada entre serviços além de `contracts/`, e cada worker precisa ser construído e medido isolado.
