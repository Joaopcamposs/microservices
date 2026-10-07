-- Executado pelo Postgres na primeira inicialização (docker-entrypoint-initdb.d).

CREATE TABLE jobs (
    job_id     UUID        PRIMARY KEY,
    type       TEXT        NOT NULL,
    target     TEXT        NOT NULL CHECK (target IN ('all', 'celery', 'taskiq', 'asyncio', 'go')),
    origin     TEXT        NOT NULL CHECK (origin IN ('gateway-py', 'gateway-go')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Um resultado por worker: no modo fanout o mesmo job roda nas 4 stacks.
-- A chave composta também torna a gravação idempotente (entrega at-least-once).
CREATE TABLE job_results (
    job_id      UUID        NOT NULL REFERENCES jobs (job_id),
    worker      TEXT        NOT NULL CHECK (worker IN ('celery', 'taskiq', 'asyncio', 'go')),
    status      TEXT        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    result      JSONB,
    error       TEXT,
    PRIMARY KEY (job_id, worker)
);

-- Fila de saída transacional: o gateway grava aqui junto com `jobs`; só o outbox-relay publica.
-- routing_key vazia = exchange fanout "jobs"; preenchida = exchange direct "jobs.direct".
CREATE TABLE outbox (
    id           BIGSERIAL   PRIMARY KEY,
    job_id       UUID        NOT NULL REFERENCES jobs (job_id),
    routing_key  TEXT        NOT NULL DEFAULT '',
    envelope     JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- Índice parcial: o relay só consulta as linhas pendentes.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
