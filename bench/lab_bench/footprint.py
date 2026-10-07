"""Footprint por stack: tamanho de imagem, memória ociosa e tempo até o primeiro resultado."""

import asyncio
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from lab_bench.database import BenchDatabase
from lab_bench.load import LoadGenerator
from lab_bench.models import Worker
from lab_bench.resources import ResourceSampler
from lab_bench.scenarios import SCENARIOS

_BYTES_PER_MB = 1024 * 1024
# Tempo para o container assentar depois do restart antes de medir a memória ociosa.
_SETTLE_SECONDS = 15.0
_POLL_SECONDS = 0.25
_FIRST_RESULT_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class Footprint:
    """Medidas de footprint de uma stack de worker."""

    worker: str
    image_mb: float
    idle_mem_mb: float
    first_result_seconds: float


class FootprintMeter:
    """Mede o footprint reiniciando os containers da stack, um worker por vez."""

    def __init__(self, project_dir: Path, gateway_url: str) -> None:
        """Prepara o gerador de carga e o acesso ao banco usados na medição."""
        self._project_dir = project_dir
        self._load = LoadGenerator(gateway_url, concurrency=1)
        self._database = BenchDatabase(project_dir)

    async def measure(self, worker: Worker) -> Footprint:
        """Reinicia a stack, envia um job e mede o tempo até o resultado e a memória ociosa.

        O tempo inclui o boot do processo, a conexão ao broker/banco e a primeira execução, que
        é o que um deploy novo paga; não é só o `import` da linguagem.
        """
        services = worker.containers
        self._compose("stop", *services)
        self._compose("up", "-d", "--no-deps", *services)
        started = time.perf_counter()
        report = await self._load.submit(SCENARIOS["overhead"], worker.value, 1)
        first_result = await self._wait_first_result(worker, report.job_ids, started)

        await asyncio.sleep(_SETTLE_SECONDS)
        sampler = ResourceSampler(self._project_dir, services)
        return Footprint(
            worker=worker.value,
            image_mb=sum(self._image_bytes(service) for service in set(services)) / _BYTES_PER_MB,
            idle_mem_mb=sampler.snapshot(),
            first_result_seconds=first_result,
        )

    async def _wait_first_result(self, worker: Worker, job_ids: list[str], started: float) -> float:
        """Espera o resultado do job e devolve quantos segundos passaram desde `started`."""
        while time.perf_counter() - started < _FIRST_RESULT_TIMEOUT_SECONDS:
            found = await asyncio.to_thread(self._database.fetch_timings, worker.value, job_ids)
            if found:
                return time.perf_counter() - started
            await asyncio.sleep(_POLL_SECONDS)
        return float("nan")

    def _image_bytes(self, service: str) -> int:
        """Tamanho da imagem do serviço; bridge e worker compartilham a mesma imagem."""
        image_service = {"celery-bridge": "worker-celery", "taskiq-bridge": "worker-taskiq"}
        name = f"{self._project_dir.name}-{image_service.get(service, service)}"
        completed = subprocess.run(
            ["docker", "image", "inspect", name, "--format", "{{.Size}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(completed.stdout.strip())

    def _compose(self, *args: str) -> None:
        """Executa `docker compose` no diretório do projeto."""
        subprocess.run(
            ["docker", "compose", "--profile", "all", *args],
            cwd=self._project_dir,
            capture_output=True,
            check=True,
        )
