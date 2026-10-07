"""Handlers dos tipos de job: a lógica de negócio que cada stack precisa reproduzir igual."""

import hashlib
import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import httpx

from app.core.value_objects import JobType, JsonObject

type Handler = Callable[[JsonObject], JsonObject]

# Timeout de cada GET do `io.fetch_urls`; igual nas quatro stacks (contrato do job).
FETCH_TIMEOUT_SECONDS = 10.0

# Tamanho do digest PBKDF2 em bytes; fixo no contrato para o resultado ser comparável.
PBKDF2_DIGEST_BYTES = 32

# Constantes do LCG de 64 bits (Knuth MMIX) que gera os registros de `data.json_transform`.
# Um gerador próprio, e não `random`, garante a mesma sequência em Python e em Go.
LCG_MULTIPLIER = 6364136223846793005
LCG_INCREMENT = 1442695040888963407
LCG_MASK = (1 << 64) - 1


def handle_io_sleep(payload: JsonObject) -> JsonObject:
    """Dorme `ms` milissegundos bloqueando o processo filho do pool prefork.

    É o ponto do experimento: no Celery prefork cada tarefa ocupa um processo inteiro enquanto
    espera, então a vazão de um job I/O-bound fica limitada pela concorrência (`-c`), ao contrário
    de asyncio e goroutines. O payload já foi validado pelo schema (`ms` entre 0 e 60000).
    """
    milliseconds = int(payload["ms"])  # type: ignore[call-overload]
    time.sleep(milliseconds / 1000)
    return {"slept_ms": milliseconds}


def handle_cpu_pbkdf2(payload: JsonObject) -> JsonObject:
    """Deriva a chave PBKDF2-HMAC-SHA256 direto no processo filho.

    No prefork cada filho tem seu GIL, então o job CPU-bound escala com `-c` sem truque.
    """
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(payload["password"]).encode(),
        str(payload["salt"]).encode(),
        int(payload["iterations"]),  # type: ignore[call-overload]
        PBKDF2_DIGEST_BYTES,
    )
    return {"digest": digest.hex()}


def fetch_one(client: httpx.Client, url: str) -> JsonObject:
    """Faz um GET e resume a resposta; erro de rede propaga e o job vira `failed`."""
    response = client.get(url)
    return {"url": url, "status": response.status_code, "bytes": len(response.content)}


def handle_io_fetch_urls(payload: JsonObject) -> JsonObject:
    """Busca as URLs em paralelo com threads e devolve os resumos na ordem recebida.

    O processo filho é síncrono, então o paralelismo entre URLs vem de um pool de threads
    (uma por URL, no máximo 50 pelo schema). Sem redirects e com o mesmo timeout das outras
    stacks; qualquer falha de rede derruba o job inteiro.
    """
    urls = [str(url) for url in payload["urls"]]  # type: ignore[attr-defined]
    with (
        httpx.Client(timeout=FETCH_TIMEOUT_SECONDS, follow_redirects=False) as client,
        ThreadPoolExecutor(max_workers=len(urls)) as pool,
    ):
        results = list(pool.map(lambda url: fetch_one(client, url), urls))
    return {"results": results}


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


def handle_data_json_transform(payload: JsonObject) -> JsonObject:
    """Gera, serializa, interpreta e agrega os registros por grupo (soma e contagem).

    Serializar e interpretar de verdade é o que o job mede; `input_bytes` prova que o JSON
    intermediário foi idêntico entre as stacks (separadores compactos).
    """
    text = json.dumps(
        generate_records(int(payload["records"]), int(payload["seed"])),  # type: ignore[call-overload]
        separators=(",", ":"),
    )
    parsed = json.loads(text)
    groups: dict[str, dict[str, int]] = {}
    for record in parsed:
        totals = groups.setdefault(record["group"], {"count": 0, "sum": 0})
        totals["count"] += 1
        totals["sum"] += record["value"]
    return {"records": len(parsed), "input_bytes": len(text.encode()), "groups": groups}


def build_handlers() -> dict[JobType, Handler]:
    """Registro `tipo -> handler`. Tipo ausente aqui é recusado pela bridge (vai para a DLQ)."""
    return {
        JobType.IO_SLEEP: handle_io_sleep,
        JobType.CPU_PBKDF2: handle_cpu_pbkdf2,
        JobType.IO_FETCH_URLS: handle_io_fetch_urls,
        JobType.DATA_JSON_TRANSFORM: handle_data_json_transform,
    }
