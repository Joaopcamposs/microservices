"""Orquestra uma rodada: aquece, envia a carga, espera drenar e calcula as métricas."""

import asyncio
import logging
import subprocess
import time
from pathlib import Path

from lab_bench.database import BenchDatabase, JobTiming
from lab_bench.load import LoadGenerator, SubmitReport
from lab_bench.models import RoundResult, Scenario, Worker
from lab_bench.resources import ResourceSampler, ResourceUsage
from lab_bench.stats import percentile

log = logging.getLogger(__name__)

# Intervalo entre consultas ao banco enquanto espera a drenagem e tempo máximo de espera.
_POLL_SECONDS = 0.5
_DRAIN_TIMEOUT_SECONDS = 300.0


class BenchRunner:
    """Executa rodadas de benchmark de um worker contra um gateway."""

    def __init__(self, project_dir: Path, gateway_url: str, concurrency: int) -> None:
        """Monta o gerador de carga e o acesso ao banco."""
        self._project_dir = project_dir
        self._load = LoadGenerator(gateway_url, concurrency)
        self._database = BenchDatabase(project_dir)

    async def run_round(self, scenario: Scenario, worker: Worker, round_number: int) -> RoundResult:
        """Faz o warm-up (descartado) e uma rodada medida, e devolve as métricas dela."""
        warmup = await self._load.submit(scenario, worker.value, scenario.warmup_jobs)
        await self._wait_for_results(worker, warmup.job_ids)

        # Backlog: o worker fica pausado enquanto a carga entra e a outbox esvazia, e só então
        # é liberado. Assim a vazão medida é a do worker drenando a fila, e não a do gateway.
        self._compose("pause", *worker.containers)
        try:
            report = await self._load.submit(scenario, worker.value, scenario.jobs)
            await self._wait_for_publication(report.job_ids)
        finally:
            self._compose("unpause", *worker.containers)
        sampler = ResourceSampler(self._project_dir, worker.containers)
        sampler.start()
        timings = await self._wait_for_results(worker, report.job_ids)
        usage = sampler.stop()
        return self._summarize(scenario, worker, round_number, report, timings, usage)

    async def _wait_for_publication(self, job_ids: list[str]) -> None:
        """Espera o relay publicar todos os jobs enviados, ou estourar o prazo."""
        deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            published = await asyncio.to_thread(self._database.count_published, job_ids)
            if published >= len(job_ids):
                return
            await asyncio.sleep(_POLL_SECONDS)
        log.warning("relay não publicou todos os jobs no prazo")

    def _compose(self, *args: str) -> None:
        """Executa `docker compose` no diretório do projeto."""
        subprocess.run(
            ["docker", "compose", "--profile", "all", *args],
            cwd=self._project_dir,
            capture_output=True,
            check=True,
        )

    async def _wait_for_results(self, worker: Worker, job_ids: list[str]) -> list[JobTiming]:
        """Consulta o banco até todos os jobs terem resultado do worker, ou estourar o prazo."""
        deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
        timings: list[JobTiming] = []
        while time.monotonic() < deadline:
            timings = await asyncio.to_thread(self._database.fetch_timings, worker.value, job_ids)
            if len(timings) >= len(job_ids):
                break
            await asyncio.sleep(_POLL_SECONDS)
        else:
            log.warning("prazo de drenagem estourou: %d/%d jobs", len(timings), len(job_ids))
        return timings

    @staticmethod
    def _summarize(
        scenario: Scenario,
        worker: Worker,
        round_number: int,
        report: SubmitReport,
        timings: list[JobTiming],
        usage: ResourceUsage,
    ) -> RoundResult:
        """Converte tempos brutos em throughput, percentis e contagens de falha/perda."""
        exec_times = [t.exec_seconds for t in timings]
        e2e_times = [t.e2e_seconds for t in timings]
        lags = [t.outbox_lag_seconds for t in timings]
        wall = _wall_seconds(timings)
        return RoundResult(
            scenario=scenario.name,
            worker=worker.value,
            round=round_number,
            jobs=scenario.jobs,
            failed=sum(1 for t in timings if not t.succeeded),
            lost=scenario.jobs - len(timings),
            wall_seconds=wall,
            throughput=len(timings) / wall if wall > 0 else 0.0,
            exec_p50=percentile(exec_times, 0.50),
            exec_p95=percentile(exec_times, 0.95),
            exec_p99=percentile(exec_times, 0.99),
            e2e_p50=percentile(e2e_times, 0.50),
            e2e_p95=percentile(e2e_times, 0.95),
            e2e_p99=percentile(e2e_times, 0.99),
            outbox_lag_p95=percentile(lags, 0.95),
            cpu_pct_mean=usage.cpu_pct_mean,
            cpu_pct_max=usage.cpu_pct_max,
            mem_mb_max=usage.mem_mb_max,
        )


def _wall_seconds(timings: list[JobTiming]) -> float:
    """Janela da drenagem: do primeiro job iniciado ao último resultado gravado."""
    if not timings:
        return 0.0
    return max(t.finished_epoch for t in timings) - min(t.started_epoch for t in timings)
