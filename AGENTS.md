# AGENTS.md

Visão geral, arquitetura e plano de fases em `README.md`.

## Documentação é obrigatória

O `README.md` faz parte da entrega. **Toda mudança que afete o que ele descreve atualiza o README no mesmo passo**, antes de dar a tarefa como pronta:

- Serviço criado, renomeado, movido ou removido -> diagrama, estrutura do repositório, seção de serviços e `docker-compose`.
- Contrato alterado (`contracts/envelope.schema.json`, schemas de job) -> seção do contrato e dos jobs.
- Endpoint, comando ou parâmetro novo/alterado -> seção do serviço correspondente.
- Schema do banco (`db/migrations`) ou fluxo da outbox -> seção da outbox e do relay.
- Novo job, cenário de teste ou métrica de benchmark -> tabelas de jobs, cenários e métricas.
- Decisão com trade-off (por que X e não Y) -> README e `docs/decisoes.md`.

Ao concluir, releia o README contra o código. Doc desatualizada é bug.

Além do README, toda mudança de código entra no `CHANGELOG.md` no mesmo passo: nova entrada (ou item na entrada do dia) em `Adicionado`, `Alterado`, `Corrigido` ou `Removido`, com a fase do plano. Registre o porquê quando a decisão não for óbvia.

## Regras de código

### Tipagem
- Python: todo código é tipado (parâmetros, retornos e atributos). Sem `Any` implícito e sem `dict` solto quando um modelo cabe.
- Sintaxe moderna (Python 3.13): `list[Job]`, `X | None`, `StrEnum`, `dataclass(frozen=True, slots=True)`.
- Estruturas de dados são `dataclass` (domínio) ou `pydantic.BaseModel` (borda da API). Nada de tuplas ou dicts anônimos circulando entre camadas.
- Go: structs com tags explícitas, erros retornados e tratados (nunca ignorados com `_`), `context.Context` como primeiro parâmetro em I/O.

### Funções e métodos
- Uma responsabilidade por função. Se precisar de "e" para descrevê-la, divida.
- Pequenas, com nomes que dizem o que fazem (verbo + objeto). Sem flags booleanas que mudam o comportamento.
- Evite efeitos colaterais escondidos: quem lê o nome deve saber se há I/O, `sleep` ou escrita.

### Classes gerenciadoras
- Comportamento com estado ou dependências vive numa classe gerenciadora (`Services`, `OutboxRelay`, `JobRepository`), com dependências injetadas no construtor ou argumento. Em Go, o equivalente é uma struct com construtor `New...`.
- Nada de estado global mutável em módulo (`global`, singletons soltos). Estado de processo fica em objeto gerenciador, criado no `lifespan` (FastAPI) ou no `main` (Go).
- Funções livres só para lógica pura e sem estado.

### Código limpo
- Nomes em inglês no código; comentários e docstrings em português.
- **Projeto de estudo, código bem documentado:** todo arquivo tem docstring de módulo (o que é e por que existe), e toda classe, função e método tem docstring que diz o propósito. Constantes, queries SQL e trechos não óbvios ganham um comentário com o porquê. Arquivos que não são Python (Makefile, compose, Dockerfile, SQL) recebem comentários equivalentes. No Python, o `ruff` (regras `D1`) barra docstring ausente; o idioma e a qualidade são responsabilidade de quem escreve.
- Docstring explica o porquê e os limites, não repete o nome da função.
- Sem código morto, sem constantes soltas não usadas, sem `print` em código de biblioteca (use `logging`).
- Valores fixos de domínio vão em módulo próprio de value objects, como `StrEnum` quando forem strings enumeráveis.
- Prefira mudanças mínimas e focadas: não refatore o que não foi pedido.

### DDD (quando aplicável)
Não force DDD onde o problema é simples. Onde fizer sentido, separe domínio (regras puras), aplicação (casos de uso) e infraestrutura (banco, broker, HTTP); o domínio não importa infraestrutura.

## Padrões do projeto

- **Contrato único:** todo serviço fala o envelope de `contracts/envelope.schema.json`. Mudança de contrato e dos consumidores entra no mesmo commit.
- **Paridade:** `gateway-py` e `gateway-go` expõem a mesma API com comportamento idêntico; os quatro workers produzem o mesmo resultado para o mesmo job.
- **Outbox:** gateways nunca publicam direto no RabbitMQ. Gravam `jobs` + `outbox` na mesma transação; só o `outbox-relay` publica.
- **Idempotência:** workers aceitam o mesmo `job_id` mais de uma vez (entrega at-least-once). Resultado gravado por `(job_id, worker)`.
- **Ack explícito:** nunca confirmar a mensagem antes de gravar o resultado. Mensagem inválida vai para DLQ, sem requeue infinito.
- **Consulta via Swagger:** todo serviço HTTP em FastAPI deve poder ser testado e inspecionado pelo `/docs`, sem curl: `summary`, `description`, `tags` e exemplos (`openapi_examples`) nos endpoints, e endpoints de consulta (listagem de jobs, inspeção da outbox) sempre que surgir estado novo que valha observar.
- **Observabilidade:** propague o `traceparent` do envelope em todo hop.
- **Benchmark justo:** limites de recurso, prefetch e concorrência iguais entre stacks e registrados. Handler CPU-bound nunca roda direto no event loop.
- **Dependências por serviço:** cada serviço tem `pyproject.toml` (uv) ou `go.mod` e Dockerfile próprios; nada de dependência cruzada entre serviços além de `contracts/`.

## Testes

- Rode apenas os testes relacionados à mudança: `pytest -x --tb=short -q <arquivo>` (Python) ou `go test ./caminho/...` (Go). Nunca a suíte inteira por padrão.
- No máximo 2 tentativas no mesmo teste que falha; se continuar, pare e explique.
- Python: `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"`). Mocke HTTP com `httpx.MockTransport` e neutralize `asyncio.sleep` para não esperar delays.
- Antes de testes de integração, confirme os serviços com `docker ps`.
- Só escreva testes que protegem comportamento real (publicação via outbox, idempotência, retry, DLQ, validação do envelope, estados do worker). Sem testes que só espelham a implementação.

## Fluxo de trabalho

- Antes de dar uma tarefa como pronta: `make ruff` limpo (Python), `go vet`/`gofmt` limpos (Go) e o fluxo afetado exercitado (endpoint, worker ou teste).
- Nunca faça commit ou push, essa responsabilidade é do humano.
