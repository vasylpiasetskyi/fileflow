import asyncio
import hashlib

import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from app.config import get_settings
from app.models import TaskResult, TaskStatus
from app.services.processing_service import ProcessingService

pytestmark = pytest.mark.integration


def _service(app: FastAPI, worker_id: str = "test-worker") -> ProcessingService:
    settings = get_settings().model_copy(update={"worker_id": worker_id})
    return ProcessingService(app.state.sessionmaker, app.state.storage, settings)


async def _upload(client, content: bytes, name: str = "doc.txt") -> dict:
    resp = await client.post("/files", files={"file": (name, content)})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def test_nothing_to_do_returns_none(app):
    assert await _service(app).process_next() is None


async def test_worker_processes_a_pending_task_and_stores_the_result(app, client):
    content = b"first line\nsecond line here\nthird"
    uploaded = await _upload(client, content)

    task = await _service(app, "worker-a").process_next()

    assert task.id.hex == uploaded["task"]["id"].replace("-", "")
    assert task.status == TaskStatus.COMPLETED
    body = (await client.get(f"/tasks/{task.id}")).json()
    assert body["status"] == "COMPLETED"
    assert body["attempt"] == 1
    assert body["worker_id"] == "worker-a"
    assert body["result"]["size"] == len(content)
    assert body["result"]["sha256"] == hashlib.sha256(content).hexdigest()
    assert body["result"]["line_count"] == 2
    assert body["result"]["word_count"] == 6


async def test_tasks_are_claimed_oldest_first(app, client):
    first = await _upload(client, b"1")
    second = await _upload(client, b"2")
    service = _service(app)

    assert str((await service.process_next()).id) == first["task"]["id"]
    assert str((await service.process_next()).id) == second["task"]["id"]


async def test_tampered_object_fails_the_checksum_check(app, client, s3):
    uploaded = await _upload(client, b"original bytes")
    await s3.put_object(
        Bucket=get_settings().s3_bucket, Key=uploaded["file"]["storage_key"], Body=b"tampered"
    )

    task = await _service(app).process_next()

    assert task.status == TaskStatus.FAILED
    assert task.error.startswith("ChecksumMismatchError")
    assert (await client.get(f"/tasks/{task.id}")).json()["result"] is None


async def test_missing_object_fails_the_task(app, client, s3):
    uploaded = await _upload(client, b"soon gone")
    await s3.delete_object(Bucket=get_settings().s3_bucket, Key=uploaded["file"]["storage_key"])

    task = await _service(app).process_next()

    assert task.status == TaskStatus.FAILED
    assert task.error.startswith("ObjectNotFoundError")


async def test_concurrent_workers_process_every_task_exactly_once(app, client):
    for i in range(12):
        await _upload(client, f"file number {i}\n".encode())
    workers = [_service(app, f"worker-{n}") for n in range(3)]

    async def drain(service: ProcessingService) -> list[str]:
        done = []
        while (task := await service.process_next()) is not None:
            done.append(str(task.id))
        return done

    processed = await asyncio.gather(*(drain(w) for w in workers))

    all_ids = [task_id for batch in processed for task_id in batch]
    assert len(all_ids) == 12
    assert len(set(all_ids)) == 12  # no task processed twice
    async with app.state.sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(TaskResult)) == 12
    # SKIP LOCKED spreads the work instead of serializing it on one worker.
    assert sum(1 for batch in processed if batch) >= 2
