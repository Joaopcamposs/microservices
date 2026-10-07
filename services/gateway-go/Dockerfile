# Imagem do gateway-go. O contexto de build é a raiz do repositório (ver docker-compose.yml),
# porque a imagem também precisa de `contracts/`, que fica fora de services/gateway-go.
FROM golang:1.27 AS build
WORKDIR /src
# go.mod/go.sum antes do código: o cache do Docker só refaz o download se as dependências mudarem.
COPY services/gateway-go/go.mod services/gateway-go/go.sum ./
RUN go mod download
COPY services/gateway-go/ .
# CGO desligado gera binário estático, que roda na imagem distroless sem libc.
RUN CGO_ENABLED=0 go build -o /out/gateway-go ./cmd/gateway-go

# Imagem final mínima: só o binário e os contratos, sem shell nem gerenciador de pacotes.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/gateway-go /gateway-go
COPY contracts /contracts
ENV GATEWAY_CONTRACTS_DIR=/contracts GIN_MODE=release
ENTRYPOINT ["/gateway-go"]
