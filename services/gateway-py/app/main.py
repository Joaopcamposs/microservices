"""Ponto de entrada: monta a aplicação FastAPI (`app.main:app` no uvicorn)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from app.api.routes import router
from app.core.settings import Settings
from app.domain.payload_validator import InvalidPayloadError, UnsupportedJobTypeError
from app.services.container import Services


def create_app(settings: Settings | None = None) -> FastAPI:
    """Fábrica da aplicação: registra rotas, métricas e o tratamento de erros de contrato.

    Ser uma função (e não só um `app` global) permite aos testes criar instâncias isoladas.
    """
    resolved = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Cria os serviços no startup e libera o pool de conexões no shutdown."""
        services = Services(resolved)
        app.state.services = services
        yield
        await services.close()

    app = FastAPI(
        title="gateway-py",
        description="Gateway do Microservices Lab. A UI de teste é esta página (`/docs`).",
        lifespan=lifespan,
    )
    app.include_router(router)
    app.mount("/metrics", make_asgi_app())

    @app.exception_handler(InvalidPayloadError)
    @app.exception_handler(UnsupportedJobTypeError)
    async def reject_job(_: Request, error: Exception) -> JSONResponse:
        """Traduz violação do contrato (payload inválido ou tipo sem schema) em `422`."""
        return JSONResponse({"detail": str(error)}, status.HTTP_422_UNPROCESSABLE_ENTITY)

    return app


# Instância usada pelo uvicorn.
app = create_app()
