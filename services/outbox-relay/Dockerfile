# Imagem do outbox-relay. Contexto de build: ./services/outbox-relay (o relay não usa contracts/,
# pois repassa o envelope sem interpretá-lo).
FROM golang:1.27 AS build
WORKDIR /src
# go.mod/go.sum antes do código: o cache do Docker só refaz o download se as dependências mudarem.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO desligado gera binário estático, que roda na imagem distroless sem libc.
RUN CGO_ENABLED=0 go build -o /out/outbox-relay ./cmd/outbox-relay

# Imagem final mínima: só o binário, sem shell nem gerenciador de pacotes.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/outbox-relay /outbox-relay
ENTRYPOINT ["/outbox-relay"]
