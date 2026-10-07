"""Benchmark dos gateways: vazão e latência HTTP do `POST /jobs` (caminho do outbox)."""

from dataclasses import dataclass
from statistics import median

from lab_bench.load import LoadGenerator
from lab_bench.scenarios import SCENARIOS
from lab_bench.stats import percentile

# Job barato, só para a requisição medir o gateway (validação + transação jobs/outbox) e não
# o handler; os jobs seguem para o worker-go, que drena rápido.
_SCENARIO = SCENARIOS["overhead"]
_TARGET = "go"
_JOBS = 2000
_WARMUP_JOBS = 200


@dataclass(frozen=True, slots=True)
class GatewayResult:
    """Mediana entre rodadas da vazão e dos percentis de latência de um gateway."""

    gateway: str
    rounds: int
    errors: int
    requests_per_second: float
    http_p50_ms: float
    http_p95_ms: float
    http_p99_ms: float


async def measure_gateway(name: str, url: str, concurrency: int, rounds: int) -> GatewayResult:
    """Mede um gateway: um warm-up descartado e `rounds` rodadas de `_JOBS` POSTs."""
    load = LoadGenerator(url, concurrency)
    await load.submit(_SCENARIO, _TARGET, _WARMUP_JOBS)
    rates: list[float] = []
    p50s: list[float] = []
    p95s: list[float] = []
    p99s: list[float] = []
    errors = 0
    for _ in range(rounds):
        report = await load.submit(_SCENARIO, _TARGET, _JOBS)
        errors += report.errors
        rates.append(len(report.job_ids) / report.seconds)
        p50s.append(percentile(report.latencies, 0.50))
        p95s.append(percentile(report.latencies, 0.95))
        p99s.append(percentile(report.latencies, 0.99))
    return GatewayResult(
        gateway=name,
        rounds=rounds,
        errors=errors,
        requests_per_second=median(rates),
        http_p50_ms=median(p50s) * 1000,
        http_p95_ms=median(p95s) * 1000,
        http_p99_ms=median(p99s) * 1000,
    )
