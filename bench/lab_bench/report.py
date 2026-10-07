"""Gravação dos resultados: CSV bruto por rodada e resumo (mediana e dispersão) em Markdown."""

import csv
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, fields
from pathlib import Path

from lab_bench.models import RoundResult
from lab_bench.stats import median_and_spread


def write_csv(path: Path, results: list[RoundResult]) -> None:
    """Grava uma linha por rodada, com todos os campos de `RoundResult`."""
    names = [field.name for field in fields(RoundResult)]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))


def summary_markdown(results: list[RoundResult]) -> str:
    """Tabela por cenário com a mediana das rodadas e a dispersão (min-max) da vazão."""
    grouped: dict[tuple[str, str], list[RoundResult]] = defaultdict(list)
    for result in results:
        grouped[(result.scenario, result.worker)].append(result)

    lines: list[str] = []
    for scenario in dict.fromkeys(result.scenario for result in results):
        lines += [
            f"#### {scenario}",
            "",
            "| worker | rodadas | jobs/s (mediana) | ± | exec p50 | exec p95 | exec p99 "
            "| e2e p95 | CPU% médio | mem máx MB | falhas | perdas |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for (name, worker), rounds in grouped.items():
            if name != scenario:
                continue
            lines.append(_summary_row(worker, rounds))
        lines.append("")
    return "\n".join(lines)


def _summary_row(worker: str, rounds: list[RoundResult]) -> str:
    """Uma linha da tabela de resumo para um worker."""
    throughput, spread = median_and_spread([r.throughput for r in rounds])

    def med(extract: Callable[[RoundResult], float]) -> float:
        """Mediana de uma métrica entre as rodadas."""
        return median_and_spread([extract(r) for r in rounds])[0]

    return (
        f"| {worker} | {len(rounds)} | {throughput:.1f} | {spread:.1f} "
        f"| {med(lambda r: r.exec_p50) * 1000:.0f} ms | {med(lambda r: r.exec_p95) * 1000:.0f} ms "
        f"| {med(lambda r: r.exec_p99) * 1000:.0f} ms | {med(lambda r: r.e2e_p95):.2f} s "
        f"| {med(lambda r: r.cpu_pct_mean):.0f} | {med(lambda r: r.mem_mb_max):.0f} "
        f"| {sum(r.failed for r in rounds)} | {sum(r.lost for r in rounds)} |"
    )
