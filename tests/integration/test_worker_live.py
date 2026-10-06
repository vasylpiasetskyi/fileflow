import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator

import pytest
from aiokafka import AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI
from sqlalchemy import func, select

from app.config import Settings, get_settings
from app.events import TaskQueuedEvent
from app.kafka import MissingTopicError
from app.models import TaskResult, TaskStatus
from app.services.processing_service import ProcessingService
from app.services.task_service import TaskTransitionConflictError
from app.worker.__main__ import run

pytestmark = pytest.mark.integration


def _service(app: FastAPI, worker_id: str = "test-worker") -> ProcessingService:
    settings = get_settings().model_copy(update={"worker_id": worker_id})
    return ProcessingService(app.state.sessionmaker, app.state.storage, settings)


async def _upload(client, content: bytes, name: str = "doc.txt") -> dict:
    resp = await client.post("/files", files={"file": (name, content)})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _event(uploaded: dict) -> TaskQueuedEvent:
    return TaskQueuedEvent(
        task_id=uuid.UUID(uploaded["task"]["id"]), file_id=uuid.UUID(uploaded["file"]["id"])
    )


async def _wait_for_status(client, task_id: str, status: str) -> dict:
    """Poll the API until the task reaches `status` (no fixed sleeps)."""
    async with asyncio.timeout(20):
        while True:
            body = (await client.get(f"/tasks/{task_id}")).json()
            if body["status"] == status:
                return body
            await asyncio.sleep(0.1)


@pytest.fixture
async def worker() -> AsyncIterator[Settings]:
    """The real worker loop (consumer + processing) running in the background."""
    settings = get_settings().model_copy(update={"worker_id": "bg-worker"})
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, stop))
    yield settings
    stop.set()
    await asyncio.wait_for(task, timeout=10)


# --- ProcessingService, driven directly with events ---------------------------


async def test_processing_an_event_stores_the_result(app, client):
    content = b"first line\nsecond line here\nthird"
    uploaded = await _upload(client, content)

    task = await _service(app, "worker-a").process(_event(uploaded))

    assert task.status == TaskStatus.COMPLETED
    body = (await client.get(f"/tasks/{task.id}")).json()
    assert body["attempt"] == 1
    assert body["worker_id"] == "worker-a"
    assert body["result"]["size"] == len(content)
    assert body["result"]["sha256"] == hashlib.sha256(content).hexdigest()
    assert body["result"]["line_count"] == 2
    assert body["result"]["word_count"] == 6


async def test_same_event_twice_is_rejected_by_the_state_machine(app, client):
    uploaded = await _upload(client, b"once")
    service = _service(app)
    await service.process(_event(uploaded))

    with pytest.raises(TaskTransitionConflictError):
        await service.process(_event(uploaded))

    async with app.state.sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(TaskResult)) == 1


async def test_tampered_object_fails_the_checksum_check(app, client, s3):
    uploaded = await _upload(client, b"original bytes")
    await s3.put_object(
        Bucket=get_settings().s3_bucket, Key=uploaded["file"]["storage_key"], Body=b"tampered"
    )

    task = await _service(app).process(_event(uploaded))

    assert task.status == TaskStatus.FAILED
    assert task.error.startswith("ChecksumMismatchError")


async def test_missing_object_fails_the_task(app, client, s3):
    uploaded = await _upload(client, b"soon gone")
    await s3.delete_object(Bucket=get_settings().s3_bucket, Key=uploaded["file"]["storage_key"])

    task = await _service(app).process(_event(uploaded))

    assert task.status == TaskStatus.FAILED
    assert task.error.startswith("ObjectNotFoundError")


# --- The real worker: Kafka consumer end to end -------------------------------


async def test_worker_processes_uploads_end_to_end_and_commits_offsets(client, worker):
    uploads = [await _upload(client, f"file {i}\n".encode()) for i in range(3)]

    for uploaded in uploads:
        body = await _wait_for_status(client, uploaded["task"]["id"], "COMPLETED")
        assert body["worker_id"] == worker.worker_id

    # Offsets are committed after processing: the group has consumed all 3.
    admin = AIOKafkaAdminClient(bootstrap_servers=worker.kafka_bootstrap_servers)
    await admin.start()
    try:
        async with asyncio.timeout(10):
            while True:
                offsets = await admin.list_consumer_group_offsets(worker.kafka_consumer_group)
                committed = offsets.get(TopicPartition(worker.kafka_topic, 0))
                if committed is not None and committed.offset == 3:
                    break
                await asyncio.sleep(0.1)
    finally:
        await admin.close()


async def test_worker_skips_a_poison_message_and_keeps_going(client, worker):
    producer = AIOKafkaProducer(bootstrap_servers=worker.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(worker.kafka_topic, b"this is not an event")
    finally:
        await producer.stop()

    uploaded = await _upload(client, b"after the poison message")

    await _wait_for_status(client, uploaded["task"]["id"], "COMPLETED")


async def test_worker_refuses_to_start_on_a_missing_topic():
    settings = get_settings().model_copy(update={"kafka_topic": "no-such-topic"})

    with pytest.raises(MissingTopicError, match="no-such-topic"):
        await run(settings, asyncio.Event())
