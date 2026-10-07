"""Estatística pura do benchmark: percentis e agregação entre rodadas."""

import math
import statistics


def percentile(values: list[float], fraction: float) -> float:
    """Percentil por ranking mais próximo (`fraction` entre 0 e 1); lista vazia dá 0.

    Escolhido em vez de interpolação para o valor reportado ser sempre uma amostra real.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(math.ceil(fraction * len(ordered)), 1)
    return ordered[rank - 1]


def median_and_spread(values: list[float]) -> tuple[float, float]:
    """Mediana e dispersão (min-max) de uma métrica entre rodadas, como o README pede."""
    if not values:
        return 0.0, 0.0
    return statistics.median(values), max(values) - min(values)
