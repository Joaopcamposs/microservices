# Imagem do worker-go. O contexto de build é a raiz do repositório (ver docker-compose.yml),
# porque a imagem também precisa de `contracts/`, que fica fora de services/worker-go.
FROM golang:1.27 AS build
WORKDIR /src
# go.mod/go.sum antes do código: o cache do Docker só refaz o download se as dependências mudarem.
COPY services/worker-go/go.mod services/worker-go/go.sum ./
RUN go mod download
COPY services/worker-go/ .
# CGO desligado gera binário estático, que roda na imagem distroless sem libc.
RUN CGO_ENABLED=0 go build -o /out/worker-go ./cmd/worker-go

# Imagem final mínima: só o binário e os contratos, sem shell nem gerenciador de pacotes.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/worker-go /worker-go
COPY contracts /contracts
ENV WORKER_CONTRACTS_DIR=/contracts
ENTRYPOINT ["/worker-go"]
