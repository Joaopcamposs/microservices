"""Handlers dos tipos de job: a lógica de negócio que cada stack precisa reproduzir igual."""

import time
from collections.abc import Callable

from app.core.value_objects import JobType, JsonObject

type Handler = Callable[[JsonObject], JsonObject]


def handle_io_sleep(payload: JsonObject) -> JsonObject:
    """Dorme `ms` milissegundos bloqueando o processo filho do pool prefork.

    É o ponto do experimento: no Celery prefork cada tarefa ocupa um processo inteiro enquanto
    espera, então a vazão de um job I/O-bound fica limitada pela concorrência (`-c`), ao contrário
    de asyncio e goroutines. O payload já foi validado pelo schema (`ms` entre 0 e 60000).
    """
    milliseconds = int(payload["ms"])  # type: ignore[call-overload]
    time.sleep(milliseconds / 1000)
    return {"slept_ms": milliseconds}


def build_handlers() -> dict[JobType, Handler]:
    """Registro `tipo -> handler`. Tipo ausente aqui é recusado pela bridge (vai para a DLQ)."""
    return {JobType.IO_SLEEP: handle_io_sleep}
