import asyncio
import hashlib
import uuid

import pytest
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from sqlalchemy import select

from app.api.deps import get_file_service
from app.config import get_settings
from app.events import TaskQueuedEvent
from app.kafka import PublishError
from app.models import File, FileStatus, Task, TaskStatus
from app.services.file_service import FileService
from app.storage import StorageError

pytestmark = pytest.mark.integration


async def _read_object(s3, key: str) -> bytes:
    obj = await s3.get_object(Bucket=get_settings().s3_bucket, Key=key)
    async with obj["Body"] as body:
        return await body.read()


async def test_upload_stores_metadata_in_postgres_and_bytes_in_minio(client, s3):
    content = b"line one\nline two\n"

    resp = await client.post("/files", files={"file": ("notes.txt", content, "text/plain")})

    assert resp.status_code == 201, resp.text
    body = resp.json()["file"]
    assert body["status"] == "UPLOADED"
    assert body["filename"] == "notes.txt"
    assert body["content_type"] == "text/plain"
    assert body["size"] == len(content)
    assert body["checksum_sha256"] == hashlib.sha256(content).hexdigest()
    assert body["storage_key"] == f"files/{body['id']}"
    assert await _read_object(s3, body["storage_key"]) == content


async def test_get_file_returns_the_uploaded_metadata(client):
    created = (await client.post("/files", files={"file": ("a.bin", b"\x00\x01")})).json()["file"]

    resp = await client.get(f"/files/{created['id']}")

    assert resp.status_code == 200
    assert resp.json() == created


async def test_get_unknown_file_returns_404(client):
    resp = await client.get(f"/files/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_multi_part_upload_reassembles_the_exact_bytes(client, s3):
    # Deterministic ~20 MiB payload -> 3 parts with the default 8 MiB part size.
    content = bytes(range(256)) * (20 * 1024 * 4)

    resp = await client.post("/files", files={"file": ("big.bin", content)})

    assert resp.status_code == 201, resp.text
    body = resp.json()["file"]
    assert body["size"] == len(content)
    assert body["checksum_sha256"] == hashlib.sha256(content).hexdigest()
    assert await _read_object(s3, body["storage_key"]) == content


async def test_empty_file_is_a_valid_upload(client, s3):
    resp = await client.post("/files", files={"file": ("empty.txt", b"")})

    assert resp.status_code == 201, resp.text
    assert resp.json()["file"]["size"] == 0
    assert await _read_object(s3, resp.json()["file"]["storage_key"]) == b""


class _UnavailableStorage:
    async def upload_stream(self, key, chunks, content_type) -> None:
        raise StorageError("connection refused")


async def test_storage_failure_returns_503_and_marks_file_failed(app: FastAPI, client):
    sessionmaker = app.state.sessionmaker
    app.dependency_overrides[get_file_service] = lambda: FileService(
        sessionmaker, _UnavailableStorage(), app.state.publisher
    )
    filename = f"doomed-{uuid.uuid4()}.txt"

    resp = await client.post("/files", files={"file": (filename, b"data")})

    assert resp.status_code == 503
    async with sessionmaker() as session:
        file = await session.scalar(select(File).where(File.filename == filename))
        tasks = (await session.scalars(select(Task).where(Task.file_id == file.id))).all()
    # The failure is recorded, not lost: the row exists and says FAILED.
    assert file is not None
    assert file.status == FileStatus.FAILED
    assert file.size is None
    # Nothing to process: a task exists only for an UPLOADED file.
    assert tasks == []


async def test_upload_publishes_a_task_queued_event(client, kafka_topic):
    body = (await client.post("/files", files={"file": ("e.txt", b"event me")})).json()

    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=get_settings().kafka_bootstrap_servers,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=10)
    finally:
        await consumer.stop()

    event = TaskQueuedEvent.model_validate_json(msg.value)
    assert str(event.task_id) == body["task"]["id"]
    assert str(event.file_id) == body["file"]["id"]
    assert event.attempt == 1
    assert msg.key == body["task"]["id"].encode()


class _UnavailableKafka:
    async def publish(self, event) -> None:
        raise PublishError("kafka unavailable")


async def test_publish_failure_after_commit_leaves_an_orphan_pending_task(app: FastAPI, client):
    """Documents the dual-write flaw that Phase 06 (outbox) fixes."""
    app.state.publisher = _UnavailableKafka()
    filename = f"orphan-{uuid.uuid4()}.txt"

    resp = await client.post("/files", files={"file": (filename, b"data")})

    assert resp.status_code == 503
    async with app.state.sessionmaker() as session:
        file = await session.scalar(select(File).where(File.filename == filename))
        task = await session.scalar(select(Task).where(Task.file_id == file.id))
    # The database says "ready to process", yet no event exists: nobody will.
    assert file.status == FileStatus.UPLOADED
    assert task.status == TaskStatus.PENDING
