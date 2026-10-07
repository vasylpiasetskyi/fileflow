"""Dead letter queue: failed tasks via the outbox, rejected messages directly, redrive."""

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from sqlalchemy import select

from app.config import get_settings
from app.dlq.__main__ import list_dead_letters
from app.events import DeadLetter, DeadLetterKind, TaskQueuedEvent
from app.models import OutboxEvent, Task, TaskStatus
from app.repositories.outbox_repository import OutboxRepository
from app.services.processing_service import ProcessingService
from app.services.task_service import TaskService, TaskTransitionConflictError
from app.worker.__main__ import run as run_worker
from tests.integration.conftest import read_topic

pytestmark = pytest.mark.integration


def _service(app: FastAPI, storage=None) -> ProcessingService:
    settings = get_settings().model_copy(update={"worker_id": "w"})
    return ProcessingService(app.state.sessionmaker, storage or app.state.storage, settings)


async def _upload(client, content: bytes = b"text\n") -> TaskQueuedEvent:
    body = (await client.post("/files", files={"file": ("f.txt", content)})).json()
    return TaskQueuedEvent(task_id=body["task"]["id"], file_id=body["file"]["id"])


async def _task(app: FastAPI, task_id: uuid.UUID) -> Task:
    async with app.state.sessionmaker() as session:
        return await session.get(Task, task_id)


async def _outbox_dead_letters(app: FastAPI) -> list[DeadLetter]:
    async with app.state.sessionmaker() as session:
        rows = (
            await session.scalars(
                select(OutboxEvent).where(OutboxEvent.topic == get_settings().kafka_dlq_topic)
            )
        ).all()
    return [DeadLetter.model_validate(r.payload) for r in rows]


@pytest.fixture
async def worker(kafka_topic: str) -> AsyncIterator[None]:
    stop = asyncio.Event()
    task = asyncio.create_task(run_worker(get_settings(), stop))
    yield
    stop.set()
    await asyncio.wait_for(task, timeout=10)


async def _produce(topic: str, value: bytes) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=get_settings().kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(topic, value)
    finally:
        await producer.stop()


# --- failed tasks: dead letter written atomically with FAILED (outbox) ---------


async def test_failed_task_gets_a_dead_letter_in_the_same_transaction(app, client):
    event = await _upload(client, b"binary\x00data")

    await _service(app).process(event)

    task = await _task(app, event.task_id)
    assert task.status is TaskStatus.FAILED
    [letter] = await _outbox_dead_letters(app)
    assert letter.kind is DeadLetterKind.TASK_FAILED
    assert (letter.task_id, letter.file_id, letter.attempt) == (task.id, task.file_id, 1)
    assert letter.error == task.error
    assert letter.failed_at == task.finished_at


async def test_no_dead_letter_no_failed_status(app, client, monkeypatch: pytest.MonkeyPatch):
    event = await _upload(client, b"binary\x00data")
    real_add = OutboxRepository.add

    def add_failing_for_dlq(self, topic, message):
        if isinstance(message, DeadLetter):
            raise RuntimeError("outbox insert failed")
        return real_add(self, topic, message)

    monkeypatch.setattr(OutboxRepository, "add", add_failing_for_dlq)

    with pytest.raises(RuntimeError, match="outbox insert failed"):
        await _service(app).process(event)

    # The FAILED transition rolled back together with the dead letter.
    assert (await _task(app, event.task_id)).status is TaskStatus.PROCESSING
    assert await _outbox_dead_letters(app) == []


async def test_dead_letter_reaches_the_dlq_topic(app, client, outbox_publisher):
    event = await _upload(client, b"binary\x00data")
    await _service(app).process(event)

    [msg] = await read_topic(get_settings().kafka_dlq_topic, 1)

    letter = DeadLetter.model_validate_json(msg.value)
    assert letter.task_id == event.task_id
    assert msg.key == str(event.task_id).encode()
    [listed] = await list_dead_letters(get_settings(), limit=10)  # the operator view
    assert listed == letter


# --- rejected messages: dead letter published by the worker before commit ------


async def test_poison_message_is_dead_lettered_by_the_worker(
    client, kafka_topic, outbox_publisher, worker
):
    await _produce(kafka_topic, b"\xff not json at all")

    [msg] = await read_topic(get_settings().kafka_dlq_topic, 1)

    letter = DeadLetter.model_validate_json(msg.value)
    assert letter.kind is DeadLetterKind.INVALID_MESSAGE
    assert letter.source == f"{kafka_topic}[0]@0"
    assert letter.raw_value == "� not json at all"  # invalid UTF-8, decoded leniently


async def test_event_for_unknown_task_is_dead_lettered_by_the_worker(kafka_topic, worker):
    ghost = TaskQueuedEvent(task_id=uuid.uuid4(), file_id=uuid.uuid4())
    await _produce(kafka_topic, ghost.model_dump_json().encode())

    [msg] = await read_topic(get_settings().kafka_dlq_topic, 1)

    letter = DeadLetter.model_validate_json(msg.value)
    assert (letter.kind, letter.task_id) == (DeadLetterKind.UNKNOWN_TASK, ghost.task_id)


# --- redrive: operational recovery -------------------------------------------


async def test_redrive_gives_a_failed_task_a_new_budget_without_resetting_attempt(app, client):
    event = await _upload(client)
    async with app.state.sessionmaker.begin() as session:  # simulate an exhausted task
        task = await session.get(Task, event.task_id)
        task.status, task.attempt, task.max_attempts = TaskStatus.FAILED, 5, 5

    redriven = await TaskService(app.state.sessionmaker, get_settings()).redrive(event.task_id)

    assert redriven.status is TaskStatus.PENDING
    assert redriven.attempt == 5  # the fencing token never goes back
    assert redriven.max_attempts == 5 + get_settings().task_max_attempts
    async with app.state.sessionmaker() as session:
        rows = (await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all()
    new_event = TaskQueuedEvent.model_validate(rows[-1].payload)
    assert (new_event.task_id, new_event.attempt) == (event.task_id, 6)

    result = await _service(app).process(new_event)
    assert (result.task_status, result.attempt) == (TaskStatus.COMPLETED, 6)


async def test_only_failed_tasks_can_be_redriven(app, client):
    event = await _upload(client)  # PENDING

    with pytest.raises(TaskTransitionConflictError):
        await TaskService(app.state.sessionmaker, get_settings()).redrive(event.task_id)
