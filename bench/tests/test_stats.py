"""Testes da estatística e do parse de memória, que decidem os números reportados."""

from lab_bench.resources import parse_memory_mb
from lab_bench.stats import median_and_spread, percentile


def test_percentile_returns_a_real_sample() -> None:
    """O percentil por ranking devolve uma amostra, nunca um valor interpolado."""
    values = [float(n) for n in range(1, 101)]
    assert percentile(values, 0.50) == 50.0
    assert percentile(values, 0.95) == 95.0
    assert percentile(values, 0.99) == 99.0


def test_percentile_of_empty_list_is_zero() -> None:
    """Rodada sem resultados não pode quebrar o relatório."""
    assert percentile([], 0.95) == 0.0


def test_median_and_spread() -> None:
    """A dispersão reportada é max - min entre rodadas."""
    assert median_and_spread([10.0, 12.0, 11.0]) == (11.0, 2.0)


def test_parse_memory_handles_docker_units() -> None:
    """`docker stats` mistura KiB, MiB e GiB; todos viram MB."""
    assert parse_memory_mb("512KiB / 512MiB") == 0.5
    assert parse_memory_mb("12.5MiB / 512MiB") == 12.5
    assert parse_memory_mb("1GiB / 4GiB") == 1024.0
