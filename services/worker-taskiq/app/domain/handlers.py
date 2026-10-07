"""Handlers dos tipos de job: a lógica de negócio que cada stack precisa reproduzir igual."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable

import httpx

from app.core.value_objects import JobType, JsonObject

type Handler = Callable[[JsonObject], Awaitable[JsonObject]]

# Timeout de cada GET do `io.fetch_urls`; igual nas quatro stacks (contrato do job).
FETCH_TIMEOUT_SECONDS = 10.0

# Tamanho do digest PBKDF2 em bytes; fixo no contrato para o resultado ser comparável.
PBKDF2_DIGEST_BYTES = 32

# Constantes do LCG de 64 bits (Knuth MMIX) que gera os registros de `data.json_transform`.
# Um gerador próprio, e não `random`, garante a mesma sequência em Python e em Go.
LCG_MULTIPLIER = 6364136223846793005
LCG_INCREMENT = 1442695040888963407
LCG_MASK = (1 << 64) - 1


async def handle_io_sleep(payload: JsonObject) -> JsonObject:
    """Dorme `ms` milissegundos sem bloquear o event loop.

    Mede o overhead puro do framework: o trabalho é só espera, então toda diferença de
    vazão entre as stacks vem do consumo da fila, não do handler. O payload já foi validado
    pelo schema, por isso `ms` é um inteiro entre 0 e 60000.
    """
    milliseconds = int(payload["ms"])  # type: ignore[call-overload]
    await asyncio.sleep(milliseconds / 1000)
    return {"slept_ms": milliseconds}


async def handle_cpu_pbkdf2(payload: JsonObject) -> JsonObject:
    """Deriva a chave PBKDF2-HMAC-SHA256 numa thread, para não travar o event loop.

    O `hashlib` solta o GIL durante a derivação, então threads dão paralelismo real aqui;
    o ponto do job é medir como cada stack distribui trabalho CPU-bound.
    """
    digest = await asyncio.to_thread(
        hashlib.pbkdf2_hmac,
        "sha256",
        str(payload["password"]).encode(),
        str(payload["salt"]).encode(),
        int(payload["iterations"]),  # type: ignore[call-overload]
        PBKDF2_DIGEST_BYTES,
    )
    return {"digest": digest.hex()}


async def fetch_one(client: httpx.AsyncClient, url: str) -> JsonObject:
    """Faz um GET e resume a resposta; erro de rede propaga e o job vira `failed`."""
    response = await client.get(url)
    return {"url": url, "status": response.status_code, "bytes": len(response.content)}


async def handle_io_fetch_urls(payload: JsonObject) -> JsonObject:
    """Busca todas as URLs ao mesmo tempo e devolve os resumos na ordem recebida.

    Sem redirects e com timeout fixo, para as stacks se comportarem igual. Qualquer falha de
    rede derruba o job inteiro: o resultado parcial não seria comparável entre stacks.
    """
    urls = [str(url) for url in payload["urls"]]  # type: ignore[attr-defined]
    async with httpx.AsyncClient(timeout=FETCH_TIMEOUT_SECONDS, follow_redirects=False) as client:
        results = await asyncio.gather(*(fetch_one(client, url) for url in urls))
    return {"results": list(results)}


def generate_records(count: int, seed: int) -> list[JsonObject]:
    """Gera `count` registros determinísticos a partir de `seed` com o LCG de 64 bits."""
    state = seed

    def next_value() -> int:
        nonlocal state
        state = (state * LCG_MULTIPLIER + LCG_INCREMENT) & LCG_MASK
        return state

    records: list[JsonObject] = []
    for index in range(count):
        group = (next_value() >> 33) % 16
        value = (next_value() >> 33) % 100000
        tag = next_value() >> 40
        records.append({"id": index, "group": f"g{group}", "value": value, "tag": f"t{tag:06x}"})
    return records


def transform_records(count: int, seed: int) -> JsonObject:
    """Gera, serializa, interpreta e agrega os registros por grupo (soma e contagem).

    Serializar e interpretar de verdade é o que o job mede; `input_bytes` prova que o JSON
    intermediário foi idêntico entre as stacks (separadores compactos).
    """
    text = json.dumps(generate_records(count, seed), separators=(",", ":"))
    parsed = json.loads(text)
    groups: dict[str, dict[str, int]] = {}
    for record in parsed:
        totals = groups.setdefault(record["group"], {"count": 0, "sum": 0})
        totals["count"] += 1
        totals["sum"] += record["value"]
    return {"records": len(parsed), "input_bytes": len(text.encode()), "groups": groups}


async def handle_data_json_transform(payload: JsonObject) -> JsonObject:
    """Roda a transformação numa thread: é CPU-bound e prenderia o event loop."""
    return await asyncio.to_thread(
        transform_records,
        int(payload["records"]),  # type: ignore[call-overload]
        int(payload["seed"]),  # type: ignore[call-overload]
    )


def build_handlers() -> dict[JobType, Handler]:
    """Registro `tipo -> handler`. Tipo ausente aqui vai para a DLQ (não há como processá-lo).

    Handler CPU-bound novo deve usar `asyncio.to_thread` ou processo separado, senão bloqueia
    o event loop e atrasa todas as outras mensagens em voo.
    """
    return {
        JobType.IO_SLEEP: handle_io_sleep,
        JobType.CPU_PBKDF2: handle_cpu_pbkdf2,
        JobType.IO_FETCH_URLS: handle_io_fetch_urls,
        JobType.DATA_JSON_TRANSFORM: handle_data_json_transform,
    }
