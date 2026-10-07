"""Retries: classify -> RETRY with backoff -> recovery requeues -> next attempt."""

import asyncio
import uuid
from datetime import timedelta

import pytest
from fastapi import FastAPI
from sqlalchemy import func, select, update

from app.config import get_settings
from app.events import TaskQueuedEvent
from app.models import OutboxEvent, Task, TaskStatus
from app.services.processing_service import Outcome, ProcessingService
from app.services.recovery_service import RecoveryService
from app.storage import StorageError

pytestmark = pytest.mark.integration


class FlakyStorage:
    """Delegates to the real storage, but the first `failures` downloads fail."""

    def __init__(self, real, failures: int) -> None:
        self._real = real
        self.failures_left = failures

    def download_stream(self, key: str, chunk_size: int):
        if self.failures_left > 0:
            self.failures_left -= 1
            return self._broken()
        return self._real.download_stream(key, chunk_size)

    async def _broken(self):
        raise StorageError("minio unreachable")
        yield  # makes this an async generator, like the real download_stream


def _service(app: FastAPI, storage, **overrides) -> ProcessingService:
    settings = get_settings().model_copy(update={"worker_id": "w", **overrides})
    return ProcessingService(app.state.sessionmaker, storage, settings)


def _recovery(app: FastAPI) -> RecoveryService:
    return RecoveryService(app.state.sessionmaker, get_settings().kafka_topic)


async def _upload(client, content: bytes = b"retry me\n") -> TaskQueuedEvent:
    body = (await client.post("/files", files={"file": ("r.txt", content)})).json()
    return TaskQueuedEvent(task_id=body["task"]["id"], file_id=body["file"]["id"])


async def _task(app: FastAPI, task_id: uuid.UUID) -> Task:
    async with app.state.sessionmaker() as session:
        return await session.get(Task, task_id)


async def _make_due(app: FastAPI, task_id: uuid.UUID) -> None:
    """Fast-forward: pretend the backoff delay has elapsed."""
    async with app.state.sessionmaker.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(next_attempt_at=func.now() - timedelta(seconds=1))
        )


async def _queued_events(app: FastAPI, task_id: uuid.UUID) -> list[TaskQueuedEvent]:
    async with app.state.sessionmaker() as session:
        rows = (await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all()
    events = [TaskQueuedEvent.model_validate(r.payload) for r in rows]
    return [e for e in events if e.task_id == task_id]


async def test_transient_failure_is_retried_and_then_succeeds(app, client):
    event = await _upload(client)
    flaky = FlakyStorage(app.state.storage, failures=1)
    service = _service(app, flaky)

    first = await service.process(event)

    assert (first.outcome, first.task_status) == (Outcome.PROCESSED, TaskStatus.RETRY)
    task = await _task(app, event.task_id)
    assert task.attempt == 1
    assert task.error == "StorageError: minio unreachable"
    assert task.next_attempt_at > task.finished_at  # scheduled in the future

    # Not due yet: recovery leaves it alone.
    assert await _recovery(app).requeue_due_retries(limit=10) == 0
    await _make_due(app, event.task_id)
    assert await _recovery(app).requeue_due_retries(limit=10) == 1

    task = await _task(app, event.task_id)
    assert task.status is TaskStatus.PENDING
    upload_event, retry_event = await _queued_events(app, event.task_id)
    assert (upload_event.attempt, retry_event.attempt) == (1, 2)
    assert retry_event.event_id != upload_event.event_id  # a new message, not a re-send

    second = await service.process(retry_event)

    assert (second.task_status, second.attempt) == (TaskStatus.COMPLETED, 2)
    task = await _task(app, event.task_id)
    assert task.error is None  # cleared by the successful attempt
    assert task.next_attempt_at is None


async def test_backoff_delay_follows_the_policy(app, client):
    event = await _upload(client)
    service = _service(
        app, FlakyStorage(app.state.storage, failures=1), retry_base_delay_s=60, retry_jitter=0.2
    )

    await service.process(event)

    task = await _task(app, event.task_id)
    delay = (task.next_attempt_at - task.finished_at).total_seconds()
    assert 48 <= delay <= 72  # 60s ± 20% jitter


async def test_permanent_failure_is_not_retried(app, client):
    event = await _upload(client, b"binary\x00content")

    result = await _service(app, app.state.storage).process(event)

    assert result.task_status is TaskStatus.FAILED
    task = await _task(app, event.task_id)
    assert task.attempt == 1
    assert task.next_attempt_at is None
    assert task.error.startswith("UnsupportedFormatError")


async def test_retries_stop_after_max_attempts(app, client):
    event = await _upload(client)
    async with app.state.sessionmaker.begin() as session:  # this task's budget: 2 attempts
        await session.execute(update(Task).where(Task.id == event.task_id).values(max_attempts=2))
    service = _service(app, FlakyStorage(app.state.storage, failures=99))

    assert (await service.process(event)).task_status is TaskStatus.RETRY
    await _make_due(app, event.task_id)
    await _recovery(app).requeue_due_retries(limit=10)
    retry_event = (await _queued_events(app, event.task_id))[-1]

    last = await service.process(retry_event)

    assert (last.task_status, last.attempt) == (TaskStatus.FAILED, 2)
    task = await _task(app, event.task_id)
    assert task.error == "gave up after 2 attempts: StorageError: minio unreachable"


async def test_old_event_for_a_task_waiting_to_retry_is_a_no_op(app, client):
    event = await _upload(client)
    service = _service(app, FlakyStorage(app.state.storage, failures=1))
    await service.process(event)  # -> RETRY

    again = await service.process(event)  # a late duplicate of the first event

    assert (again.outcome, again.task_status) == (Outcome.SCHEDULED, TaskStatus.RETRY)
    assert (await _task(app, event.task_id)).attempt == 1


async def test_concurrent_recovery_requeues_each_task_once(app, client):
    events = [await _upload(client) for _ in range(5)]
    service = _service(app, FlakyStorage(app.state.storage, failures=5))
    for event in events:
        await service.process(event)
        await _make_due(app, event.task_id)

    counts = await asyncio.gather(*(_recovery(app).requeue_due_retries(limit=2) for _ in range(5)))

    assert sum(counts) == 5
    for event in events:
        assert len(await _queued_events(app, event.task_id)) == 2  # upload + exactly one retry
