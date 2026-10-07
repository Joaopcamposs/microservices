# Imagem do mock-server (só stdlib). Contexto de build: ./services/mock-server.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
# CGO desligado gera binário estático, que roda na imagem distroless sem libc.
RUN CGO_ENABLED=0 go build -o /out/mock-server ./cmd/mock-server

# Imagem final mínima: só o binário.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/mock-server /mock-server
ENTRYPOINT ["/mock-server"]
