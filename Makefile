# Atalhos do projeto. Os de Python rodam em services/gateway-py e services/worker-asyncio (cada
# serviço tem seu uv); os de Go, em services/outbox-relay, gateway-go e worker-go.


# Sobe o gateway-py local com reload em :8000 (precisa de `make up` para Postgres/RabbitMQ).
run:
	cd services/gateway-py && uv run uvicorn app.main:app --reload --port 8000

# Corrige e formata o código Python com ruff (inclui a checagem de docstrings).
ruff:
	cd services/gateway-py && uv run ruff check . --fix && uv run ruff format .
	cd services/worker-asyncio && uv run ruff check . --fix && uv run ruff format .

# Formata e analisa o código Go do outbox-relay (gofmt reescreve, vet aponta problemas).
gofmt:
	cd services/outbox-relay && gofmt -w . && go vet ./...
	cd services/gateway-go && gofmt -w . && go vet ./...
	cd services/worker-go && gofmt -w . && go vet ./...

# Sobe o gateway-go local em :8001 (Swagger em /swagger; precisa de `make up`).
run-go:
	cd services/gateway-go && go run ./cmd/gateway-go

# Regenera o Swagger do gateway-go a partir das anotações dos handlers (rode após mudar a API).
swagger:
	cd services/gateway-go && go tool swag init -g cmd/gateway-go/main.go -o docs --parseInternal --outputTypes go,json

# Roda os testes; os de integração usam Postgres/RabbitMQ do compose e são pulados se estiverem fora.
test:
	cd services/gateway-py && uv run pytest -x --tb=short -q
	cd services/worker-asyncio && uv run pytest -x --tb=short -q
	cd services/outbox-relay && go test ./...
	cd services/gateway-go && go test ./...
	cd services/worker-go && go test ./...

# Sobe a infra (RabbitMQ e Postgres) e espera os healthchecks.
up:
	docker compose up -d --wait

# Para os containers, mantendo os dados.
down:
	docker compose down

# Apaga os volumes e recria tudo; use para reaplicar as migrations do zero.
reset:
	docker compose down -v && docker compose up -d --wait

# Acompanha os logs de todos os containers.
logs:
	docker compose logs -f

# Abre um shell SQL no banco `lab`.
psql:
	docker compose exec postgres psql -U postgres -d lab
