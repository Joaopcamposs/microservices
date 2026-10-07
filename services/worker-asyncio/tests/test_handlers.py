"""Testes dos handlers: vetores dourados compartilhados e busca HTTP contra servidor local."""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from app.core.value_objects import JobType, JsonObject
from app.domain.handlers import build_handlers

GOLDEN = Path(__file__).resolve().parents[3] / "contracts" / "jobs" / "examples.json"


class StubHandler(BaseHTTPRequestHandler):
    """Servidor de teste: `/ok` devolve 5 bytes, `/missing` devolve 404 com 3 bytes."""

    def do_GET(self) -> None:  # noqa: N802 - nome imposto pela biblioteca padrão
        """Responde conforme o caminho pedido."""
        status, body = (200, b"hello") if self.path == "/ok" else (404, b"nop")
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Silencia o log de acesso para não poluir a saída do pytest."""


@pytest.fixture
def stub_url() -> Iterator[str]:
    """Sobe o servidor de teste numa thread e devolve a URL base."""
    server = HTTPServer(("127.0.0.1", 0), StubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def golden_cases() -> list[tuple[str, JsonObject, JsonObject]]:
    """Lê `contracts/jobs/examples.json`: o mesmo arquivo que os workers Go e Python usam."""
    examples = json.loads(GOLDEN.read_text())["examples"]
    return [(case["type"], case["payload"], case["expected"]) for case in examples]


@pytest.mark.parametrize(("job_type", "payload", "expected"), golden_cases())
async def test_handler_matches_golden_vector(
    job_type: str, payload: JsonObject, expected: JsonObject
) -> None:
    """O resultado tem de ser idêntico ao vetor dourado, que vale para as quatro stacks."""
    handler = build_handlers()[JobType(job_type)]
    assert await handler(payload) == expected


async def test_fetch_urls_keeps_order_and_reports_status_and_size(stub_url: str) -> None:
    """Resultado na ordem recebida, com status e tamanho do corpo de cada URL."""
    urls = [f"{stub_url}/missing", f"{stub_url}/ok"]
    result = await build_handlers()[JobType.IO_FETCH_URLS]({"urls": urls})
    assert result == {
        "results": [
            {"url": urls[0], "status": 404, "bytes": 3},
            {"url": urls[1], "status": 200, "bytes": 5},
        ]
    }


async def test_fetch_urls_fails_when_connection_is_refused() -> None:
    """Erro de rede derruba o job inteiro (vira `failed` no processor)."""
    with pytest.raises(Exception):  # noqa: B017, PT011 - o tipo vem da biblioteca HTTP
        await build_handlers()[JobType.IO_FETCH_URLS]({"urls": ["http://127.0.0.1:1/x"]})
