"""Testes do caso de uso: resultado gravado, falha de handler e duplicidade."""

from pathlib import Path

import pytest

from app.core.value_objects import JobType, JsonObject, ResultStatus
from app.domain.contracts import ContractValidator, InvalidMessageError
from app.domain.models import JobResult
from app.services.job_processor import JobProcessor
from tests.test_contracts import CONTRACTS, make_body


class FakeRepository:
    """Repositório em memória com a mesma regra de idempotência do real."""

    def __init__(self) -> None:
        """Começa vazio."""
        self.saved: dict[tuple[str, str], JobResult] = {}

    async def save(self, result: JobResult) -> bool:
        """Grava só a primeira vez que vê `(job_id, worker)`."""
        key = (result.job_id, result.worker)
        if key in self.saved:
            return False
        self.saved[key] = result
        return True


async def ok_handler(payload: JsonObject) -> JsonObject:
    """Handler que sempre funciona."""
    return {"echo": payload}


async def broken_handler(payload: JsonObject) -> JsonObject:
    """Handler que sempre falha."""
    raise RuntimeError("boom")


def make_processor(handler, repository: FakeRepository) -> JobProcessor:
    """Monta o processor com o schema real e o handler dado para `io.sleep`."""
    return JobProcessor(
        ContractValidator.from_directory(Path(CONTRACTS)),
        {JobType.IO_SLEEP: handler},
        repository,  # type: ignore[arg-type]
    )


async def test_success_is_saved() -> None:
    """Handler ok grava `succeeded` com o resultado."""
    repo = FakeRepository()
    await make_processor(ok_handler, repo).process(make_body())
    (saved,) = repo.saved.values()
    assert saved.status is ResultStatus.SUCCEEDED
    assert saved.result == {"echo": {"ms": 10}}
    assert saved.worker == "taskiq"


async def test_handler_failure_is_saved_not_raised() -> None:
    """Falha do handler vira `failed` gravado; a mensagem pode ser confirmada."""
    repo = FakeRepository()
    await make_processor(broken_handler, repo).process(make_body())
    (saved,) = repo.saved.values()
    assert saved.status is ResultStatus.FAILED
    assert saved.error == "RuntimeError: boom"


async def test_duplicate_delivery_keeps_first_result() -> None:
    """A mesma mensagem duas vezes (at-least-once) não sobrescreve nem levanta erro."""
    repo = FakeRepository()
    processor = make_processor(ok_handler, repo)
    await processor.process(make_body())
    await processor.process(make_body())
    assert len(repo.saved) == 1


async def test_invalid_message_saves_nothing() -> None:
    """Mensagem inválida levanta (vai para a DLQ) e não deixa resultado."""
    repo = FakeRepository()
    with pytest.raises(InvalidMessageError):
        await make_processor(ok_handler, repo).process(b"lixo")
    assert not repo.saved
