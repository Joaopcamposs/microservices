"""Catálogo de cenários do benchmark base (fase 6)."""

from lab_bench.models import Scenario

# URLs do io.fetch_urls: 5 requisições de 200 ms em paralelo; o job dura ~200 ms se a stack
# fizer I/O concorrente de verdade e ~1 s se serializar.
_FETCH_URLS = ["http://mock-server:8090/delay/200"] * 5

SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in (
        Scenario(
            name="overhead",
            job_type="io.sleep",
            payload={"ms": 0},
            jobs=1000,
            warmup_jobs=100,
            description="Custo puro do pipeline e do framework: o handler não faz nada.",
        ),
        Scenario(
            name="cpu",
            job_type="cpu.pbkdf2",
            payload={"password": "senha", "salt": "sal", "iterations": 200_000},
            jobs=200,
            warmup_jobs=20,
            description="CPU-bound: paralelismo real, GIL e processos.",
        ),
        Scenario(
            name="io",
            job_type="io.fetch_urls",
            payload={"urls": _FETCH_URLS},
            jobs=300,
            warmup_jobs=30,
            description="I/O-bound: concorrência de I/O (5 GETs de 200 ms por job).",
        ),
        Scenario(
            name="serialization",
            job_type="data.json_transform",
            payload={"records": 5000, "seed": 42},
            jobs=200,
            warmup_jobs=20,
            description="Custo de gerar, serializar, ler e agregar JSON.",
        ),
    )
}
