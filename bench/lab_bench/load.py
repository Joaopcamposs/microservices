"""Gerador de carga: envia jobs a um gateway com concorrência fixa (laço fechado)."""

import asyncio
import time
from dataclasses import dataclass

import httpx

from lab_bench.models import Scenario


@dataclass(frozen=True, slots=True)
class SubmitReport:
    """Resultado do envio: IDs aceitos, erros e a latência HTTP de cada `POST /jobs`."""

    job_ids: list[str]
    errors: int
    latencies: list[float]
    seconds: float


class LoadGenerator:
    """Envia N jobs a um gateway com `concurrency` requisições simultâneas.

    Laço fechado: cada requisição só começa quando uma anterior termina, então o gerador mede
    a vazão que o sistema sustenta e não força um ritmo fixo de chegada.
    """

    def __init__(self, gateway_url: str, concurrency: int) -> None:
        """Guarda o gateway alvo e o nível de concorrência."""
        self._gateway_url = gateway_url
        self._concurrency = concurrency

    async def submit(self, scenario: Scenario, target: str, jobs: int) -> SubmitReport:
        """Envia `jobs` jobs do cenário para o `target` e espera todos os aceites."""
        limits = httpx.Limits(max_connections=self._concurrency)
        gate = asyncio.Semaphore(self._concurrency)
        async with httpx.AsyncClient(base_url=self._gateway_url, limits=limits) as client:
            started = time.perf_counter()
            outcomes = await asyncio.gather(
                *(self._post_one(client, gate, scenario, target) for _ in range(jobs))
            )
            seconds = time.perf_counter() - started
        accepted = [(job_id, latency) for job_id, latency in outcomes if job_id is not None]
        return SubmitReport(
            job_ids=[job_id for job_id, _ in accepted],
            errors=jobs - len(accepted),
            latencies=[latency for _, latency in accepted],
            seconds=seconds,
        )

    async def _post_one(
        self, client: httpx.AsyncClient, gate: asyncio.Semaphore, scenario: Scenario, target: str
    ) -> tuple[str | None, float]:
        """Faz um `POST /jobs` e devolve (job_id ou None se falhou, latência HTTP em s)."""
        async with gate:
            started = time.perf_counter()
            try:
                response = await client.post(
                    "/jobs",
                    params={"type": scenario.job_type, "target": target},
                    json=scenario.payload,
                )
            except httpx.HTTPError:
                return None, time.perf_counter() - started
            latency = time.perf_counter() - started
        if response.status_code != httpx.codes.ACCEPTED:
            return None, latency
        return str(response.json()["job_id"]), latency
