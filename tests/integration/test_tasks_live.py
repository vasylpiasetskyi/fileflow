import asyncio
import uuid

import pytest
from fastapi import FastAPI
from sqlalchemy import select

from app.models import File, FileStatus, Task, TaskStatus
from app.processing import FileStats
from app.repositories.task_repository import TaskRepository
from app.services.task_service import (
    TaskService,
    TaskTransitionConflictError,
    UnknownTaskError,
)

pytestmark = pytest.mark.integration

WORKER = "test-worker"
STATS = FileStats(size=13, sha256="0" * 64, line_count=1, word_count=2)


async def _upload(client, name: str = "doc.txt") -> dict:
    resp = await client.post("/files", files={"file": (name, b"some content\n")})
    assert resp.status_code == 201, resp.text
    return resp.json()


@pytest.fixture
def tasks(app: FastAPI) -> TaskService:
    return TaskService(app.state.sessionmaker)


async def test_upload_creates_a_pending_task_for_the_file(client):
    body = await _upload(client)

    task = body["task"]
    assert task["status"] == "PENDING"
    assert task["file_id"] == body["file"]["id"]
    assert task["attempt"] == 0
    assert task["started_at"] is None


async def test_get_task_returns_its_state(client):
    created = (await _upload(client))["task"]

    resp = await client.get(f"/tasks/{created['id']}")

    assert resp.status_code == 200
    assert resp.json() == {**created, "result": None}


async def test_get_unknown_task_returns_404(client):
    assert (await client.get(f"/tasks/{uuid.uuid4()}")).status_code == 404


async def test_happy_path_pending_processing_completed(client, tasks):
    task_id = uuid.UUID((await _upload(client))["task"]["id"])

    started = await tasks.start(task_id, WORKER)
    assert started.status == TaskStatus.PROCESSING
    assert started.attempt == 1
    assert started.worker_id == WORKER
    assert started.started_at is not None

    completed = await tasks.complete(task_id, STATS, processing_ms=5)
    assert completed.status == TaskStatus.COMPLETED
    assert completed.finished_at is not None
    result = await tasks.get_result(task_id)
    assert (result.size, result.line_count, result.word_count) == (13, 1, 2)


async def test_fail_records_the_error(client, tasks):
    task_id = uuid.UUID((await _upload(client))["task"]["id"])
    await tasks.start(task_id, WORKER)

    failed = await tasks.fail(task_id, "unsupported format")

    assert failed.status == TaskStatus.FAILED
    assert failed.error == "unsupported format"


async def test_illegal_transition_is_rejected_and_state_is_unchanged(client, tasks):
    task_id = uuid.UUID((await _upload(client))["task"]["id"])

    with pytest.raises(TaskTransitionConflictError) as exc_info:
        # PENDING -> COMPLETED skips PROCESSING
        await tasks.complete(task_id, STATS, processing_ms=5)

    assert exc_info.value.current == TaskStatus.PENDING
    assert (await tasks.get(task_id)).status == TaskStatus.PENDING
    assert await tasks.get_result(task_id) is None  # result rolled back with the transition


async def test_terminal_task_cannot_be_restarted(client, tasks):
    task_id = uuid.UUID((await _upload(client))["task"]["id"])
    await tasks.start(task_id, WORKER)
    await tasks.complete(task_id, STATS, processing_ms=5)

    with pytest.raises(TaskTransitionConflictError):
        await tasks.start(task_id, WORKER)
    assert (await tasks.get(task_id)).attempt == 1


async def test_transition_of_unknown_task_raises(tasks):
    with pytest.raises(UnknownTaskError):
        await tasks.start(uuid.uuid4(), WORKER)


async def test_concurrent_starts_exactly_one_wins(client, tasks):
    task_id = uuid.UUID((await _upload(client))["task"]["id"])

    # 10 claims on separate connections at the same time.
    results = await asyncio.gather(
        *(tasks.start(task_id, WORKER) for _ in range(10)), return_exceptions=True
    )

    winners = [r for r in results if isinstance(r, Task)]
    losers = [r for r in results if isinstance(r, TaskTransitionConflictError)]
    assert len(winners) == 1
    assert len(losers) == 9
    assert (await tasks.get(task_id)).attempt == 1  # not 10


async def test_task_creation_failure_rolls_back_the_uploaded_status(
    app: FastAPI, client, monkeypatch: pytest.MonkeyPatch
):
    def broken_add(self, task):
        raise RuntimeError("simulated failure inside tx2")

    monkeypatch.setattr(TaskRepository, "add", broken_add)
    filename = f"atomic-{uuid.uuid4()}.txt"

    with pytest.raises(RuntimeError, match="inside tx2"):
        await client.post("/files", files={"file": (filename, b"x")})

    async with app.state.sessionmaker() as session:
        file = await session.scalar(select(File).where(File.filename == filename))
    # tx2 rolled back as a whole: no UPLOADED file without a task.
    assert file.status == FileStatus.UPLOADING
