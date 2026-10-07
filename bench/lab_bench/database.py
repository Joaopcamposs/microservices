"""Acesso ao Postgres do compose via `psql`, para medir as rodadas sem dependência de driver."""

import subprocess
from dataclasses import dataclass
from pathlib import Path

# Uma linha por (job, worker) com os instantes que importam. `outbox_lag` usa o published_at
# gravado pelo relay: é o tempo que o job esperou na outbox antes de ir ao broker.
_TIMINGS_SQL = """
SELECT r.status,
       extract(epoch FROM r.finished_at - r.started_at),
       extract(epoch FROM r.finished_at - j.created_at),
       extract(epoch FROM o.published_at - o.created_at),
       extract(epoch FROM r.finished_at),
       extract(epoch FROM j.created_at),
       extract(epoch FROM r.started_at)
FROM job_results r
JOIN jobs j USING (job_id)
JOIN outbox o USING (job_id)
WHERE r.worker = '{worker}' AND j.job_id IN ({ids})
"""


@dataclass(frozen=True, slots=True)
class JobTiming:
    """Tempos de um job já processado por um worker, em segundos."""

    succeeded: bool
    exec_seconds: float
    e2e_seconds: float
    outbox_lag_seconds: float
    finished_epoch: float
    created_epoch: float
    started_epoch: float


class BenchDatabase:
    """Consulta `job_results` rodando `psql` dentro do container do Postgres."""

    def __init__(self, project_dir: Path) -> None:
        """Guarda o diretório do compose, de onde `docker compose exec` precisa rodar."""
        self._project_dir = project_dir

    def fetch_timings(self, worker: str, job_ids: list[str]) -> list[JobTiming]:
        """Devolve os tempos dos jobs que já têm resultado do worker (os demais ficam de fora)."""
        if not job_ids:
            return []
        ids = ",".join(f"'{job_id}'" for job_id in job_ids)
        query = _TIMINGS_SQL.format(worker=worker, ids=ids)
        output = self._psql(query)
        timings: list[JobTiming] = []
        for line in output.splitlines():
            status, exec_s, e2e_s, lag_s, finished, created, started = line.split(",")
            timings.append(
                JobTiming(
                    succeeded=status == "succeeded",
                    exec_seconds=float(exec_s),
                    e2e_seconds=float(e2e_s),
                    outbox_lag_seconds=float(lag_s),
                    finished_epoch=float(finished),
                    created_epoch=float(created),
                    started_epoch=float(started),
                )
            )
        return timings

    def count_published(self, job_ids: list[str]) -> int:
        """Quantos dos jobs já saíram da outbox para o broker (têm `published_at`)."""
        ids = ",".join(f"'{job_id}'" for job_id in job_ids)
        query = f"SELECT count(*) FROM outbox WHERE published_at IS NOT NULL AND job_id IN ({ids})"
        return int(self._psql(query).strip())

    def _psql(self, query: str) -> str:
        """Executa uma consulta e devolve as linhas em CSV sem cabeçalho."""
        command = [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "postgres", "-d", "lab", "-At", "-F", ",", "-c", query,
        ]  # fmt: skip
        completed = subprocess.run(
            command, cwd=self._project_dir, capture_output=True, text=True, check=True
        )
        return completed.stdout
