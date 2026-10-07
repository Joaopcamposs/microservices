"""Amostragem de CPU e memória dos containers com `docker stats`."""

import json
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path

_BYTES_PER_MB = 1024 * 1024
_UNITS = {"B": 1, "KiB": 1024, "MiB": _BYTES_PER_MB, "GiB": 1024 * _BYTES_PER_MB}


@dataclass(frozen=True, slots=True)
class ResourceUsage:
    """Resumo de uma janela de amostragem, somado sobre os containers da stack."""

    cpu_pct_mean: float
    cpu_pct_max: float
    mem_mb_max: float


@dataclass(slots=True)
class _Samples:
    """Amostras acumuladas pela thread de coleta."""

    cpu: list[float] = field(default_factory=list)
    mem: list[float] = field(default_factory=list)


def parse_memory_mb(text: str) -> float:
    """Converte o uso de memória do `docker stats` (ex.: `12.5MiB / 512MiB`) para MB."""
    used = text.split("/")[0].strip()
    for unit in sorted(_UNITS, key=len, reverse=True):
        if used.endswith(unit):
            return float(used.removesuffix(unit)) * _UNITS[unit] / _BYTES_PER_MB
    return 0.0


class ResourceSampler:
    """Coleta CPU% e memória dos containers de uma stack enquanto uma rodada executa.

    Soma os containers (bridge + worker) a cada amostra, porque a pergunta é quanto a stack
    inteira consome. Roda em thread própria: `docker stats` leva ~1 s por chamada.
    """

    def __init__(self, project_dir: Path, services: tuple[str, ...]) -> None:
        """Resolve os nomes de container do compose e prepara a coleta."""
        self._project_dir = project_dir
        self._names = [f"{project_dir.name}-{service}-1" for service in services]
        self._samples = _Samples()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._collect, daemon=True)

    def start(self) -> None:
        """Começa a coletar em segundo plano."""
        self._thread.start()

    def stop(self) -> ResourceUsage:
        """Para a coleta e devolve o resumo da janela."""
        self._stop.set()
        self._thread.join()
        cpu, mem = self._samples.cpu, self._samples.mem
        if not cpu:
            return ResourceUsage(0.0, 0.0, 0.0)
        return ResourceUsage(sum(cpu) / len(cpu), max(cpu), max(mem))

    def snapshot(self) -> float:
        """Memória atual da stack em MB, para medir o RSS ocioso antes da carga."""
        return self._read_once()[1]

    def _collect(self) -> None:
        """Laço da thread: uma amostra por chamada de `docker stats` até `stop`."""
        while not self._stop.is_set():
            cpu, mem = self._read_once()
            self._samples.cpu.append(cpu)
            self._samples.mem.append(mem)

    def _read_once(self) -> tuple[float, float]:
        """Lê uma amostra e devolve (CPU% somado, memória MB somada)."""
        completed = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *self._names],
            cwd=self._project_dir,
            capture_output=True,
            text=True,
            check=True,
        )
        cpu_total = mem_total = 0.0
        for line in completed.stdout.splitlines():
            row = json.loads(line)
            cpu_total += float(row["CPUPerc"].rstrip("%"))
            mem_total += parse_memory_mb(row["MemUsage"])
        return cpu_total, mem_total
