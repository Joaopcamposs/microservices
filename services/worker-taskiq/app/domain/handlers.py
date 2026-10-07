"""Handlers dos tipos de job: a lógica de negócio que cada stack precisa reproduzir igual."""

import asyncio
from collections.abc import Awaitable, Callable

from app.core.value_objects import JobType, JsonObject

type Handler = Callable[[JsonObject], Awaitable[JsonObject]]


async def handle_io_sleep(payload: JsonObject) -> JsonObject:
    """Dorme `ms` milissegundos sem bloquear o event loop.

    Mede o overhead puro do framework: o trabalho é só espera, então toda diferença de
    vazão entre as stacks vem do consumo da fila, não do handler. O payload já foi validado
    pelo schema, por isso `ms` é um inteiro entre 0 e 60000.
    """
    milliseconds = int(payload["ms"])  # type: ignore[call-overload]
    await asyncio.sleep(milliseconds / 1000)
    return {"slept_ms": milliseconds}


def build_handlers() -> dict[JobType, Handler]:
    """Registro `tipo -> handler`. Tipo ausente aqui vai para a DLQ (não há como processá-lo).

    Handler CPU-bound novo deve usar `asyncio.to_thread` ou processo separado, senão bloqueia
    o event loop e atrasa todas as outras mensagens em voo.
    """
    return {JobType.IO_SLEEP: handle_io_sleep}
