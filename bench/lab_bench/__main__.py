"""CLI do benchmark: `uv run python -m lab_bench run|footprint`."""

import argparse
import asyncio
import logging
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from lab_bench.footprint import Footprint, FootprintMeter
from lab_bench.gateway import measure_gateway
from lab_bench.models import RoundResult, Worker
from lab_bench.report import summary_markdown, write_csv
from lab_bench.runner import BenchRunner
from lab_bench.scenarios import SCENARIOS

# A raiz do repositório é onde o compose roda; bench/ fica um nível abaixo.
_PROJECT_DIR = Path(__file__).resolve().parents[2]
_RESULTS_DIR = _PROJECT_DIR / "bench" / "results"
_GATEWAYS = {"go": "http://localhost:8001", "py": "http://localhost:8000"}


def _parse_args() -> argparse.Namespace:
    """Define e lê os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(prog="lab_bench", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="roda cenários de carga nos workers")
    run.add_argument("--scenarios", nargs="+", default=list(SCENARIOS), choices=list(SCENARIOS))
    run.add_argument("--workers", nargs="+", default=[w.value for w in Worker],
                     choices=[w.value for w in Worker])  # fmt: skip
    run.add_argument("--rounds", type=int, default=5, help="repetições por cenário e worker")
    run.add_argument("--gateway", choices=list(_GATEWAYS), default="go")
    run.add_argument("--concurrency", type=int, default=64, help="POSTs simultâneos")

    foot = sub.add_parser("footprint", help="imagem, memória ociosa e tempo até o 1º resultado")
    foot.add_argument("--workers", nargs="+", default=[w.value for w in Worker],
                      choices=[w.value for w in Worker])  # fmt: skip
    foot.add_argument("--gateway", choices=list(_GATEWAYS), default="go")

    gate = sub.add_parser("gateway", help="vazão e latência HTTP do POST /jobs dos dois gateways")
    gate.add_argument("--rounds", type=int, default=5)
    gate.add_argument("--concurrency", type=int, default=64)
    return parser.parse_args()


def _commit_hash() -> str:
    """Hash curto do commit atual, gravado junto dos resultados para reprodutibilidade."""
    completed = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=_PROJECT_DIR, capture_output=True, text=True, check=False,
    )  # fmt: skip
    return completed.stdout.strip() or "unknown"


async def _run(args: argparse.Namespace) -> None:
    """Roda todas as combinações cenário x worker x rodada e grava CSV e resumo."""
    runner = BenchRunner(_PROJECT_DIR, _GATEWAYS[args.gateway], args.concurrency)
    results: list[RoundResult] = []
    for name in args.scenarios:
        for worker_name in args.workers:
            for round_number in range(1, args.rounds + 1):
                result = await runner.run_round(SCENARIOS[name], Worker(worker_name), round_number)
                results.append(result)
                print(
                    f"{name:14} {worker_name:8} r{round_number} "
                    f"{result.throughput:8.1f} jobs/s  exec p95 {result.exec_p95 * 1000:7.0f} ms "
                    f"falhas={result.failed} perdas={result.lost}",
                    flush=True,
                )
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    base = _RESULTS_DIR / f"{stamp}-{_commit_hash()}"
    write_csv(base.with_suffix(".csv"), results)
    base.with_suffix(".md").write_text(summary_markdown(results))
    print(f"\nResultados em {base}.csv e {base}.md")


async def _footprint(args: argparse.Namespace) -> None:
    """Mede o footprint de cada stack e imprime uma tabela Markdown."""
    meter = FootprintMeter(_PROJECT_DIR, _GATEWAYS[args.gateway])
    rows: list[Footprint] = [await meter.measure(Worker(name)) for name in args.workers]
    print("| worker | imagem (MB) | memória ociosa (MB) | 1º resultado (s) |")
    print("|---|---|---|---|")
    for row in rows:
        print(
            f"| {row.worker} | {row.image_mb:.0f} | {row.idle_mem_mb:.0f} "
            f"| {row.first_result_seconds:.1f} |"
        )


async def _gateway(args: argparse.Namespace) -> None:
    """Mede os dois gateways em sequência e imprime uma tabela Markdown."""
    print("| gateway | rodadas | POST/s | HTTP p50 | HTTP p95 | HTTP p99 | erros |")
    print("|---|---|---|---|---|---|---|")
    for name, url in _GATEWAYS.items():
        r = await measure_gateway(f"gateway-{name}", url, args.concurrency, args.rounds)
        print(
            f"| {r.gateway} | {r.rounds} | {r.requests_per_second:.0f} | {r.http_p50_ms:.0f} ms "
            f"| {r.http_p95_ms:.0f} ms | {r.http_p99_ms:.0f} ms | {r.errors} |",
            flush=True,
        )


def main() -> None:
    """Ponto de entrada da CLI."""
    logging.basicConfig(level=logging.INFO)
    # Uma linha de log por POST afogaria o resultado.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args = _parse_args()
    handlers = {"run": _run, "footprint": _footprint, "gateway": _gateway}
    asyncio.run(handlers[args.command](args))


if __name__ == "__main__":
    main()
