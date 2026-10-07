"""Transactional outbox: atomic write in the API, relay to Kafka, failure modes."""

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from sqlalchemy import select

from app.config import get_settings
from app.events import TaskQueuedEvent
from app.kafka import EventPublisher, PublishError
from app.models import File, FileStatus, OutboxEvent, OutboxStatus, Task
from app.repositories.outbox_repository import OutboxRepository
from app.services.outbox_relay import OutboxRelay
from tests.integration.conftest import read_topic

pytestmark = pytest.mark.integration


@pytest.fixture
async def publisher(kafka_topic: str) -> AsyncIterator[EventPublisher]:
    publisher = EventPublisher(get_settings())
    await publisher.start()
    yield publisher
    await publisher.stop()


def _relay(app: FastAPI, publisher, **overrides) -> OutboxRelay:
    settings = get_settings().model_copy(update=overrides)
    return OutboxRelay(app.state.sessionmaker, publisher, settings)


async def _upload(client, n: int = 1) -> list[dict]:
    bodies = []
    for i in range(n):
        resp = await client.post("/files", files={"file": (f"f{i}.txt", b"x")})
        assert resp.status_code == 201, resp.text
        bodies.append(resp.json())
    return bodies


async def _outbox(app: FastAPI) -> list[OutboxEvent]:
    async with app.state.sessionmaker() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


async def test_outbox_insert_failure_rolls_back_file_and_task(
    app: FastAPI, client, monkeypatch: pytest.MonkeyPatch
):
    def broken_add(self, topic, event):
        raise RuntimeError("simulated failure writing the outbox row")

    monkeypatch.setattr(OutboxRepository, "add", broken_add)
    filename = f"atomic-{uuid.uuid4()}.txt"

    with pytest.raises(RuntimeError, match="outbox row"):
        await client.post("/files", files={"file": (filename, b"x")})

    async with app.state.sessionmaker() as session:
        file = await session.scalar(select(File).where(File.filename == filename))
        tasks = (await session.scalars(select(Task).where(Task.file_id == file.id))).all()
    # No event -> no task either: the file was never marked UPLOADED.
    assert file.status == FileStatus.UPLOADING
    assert tasks == []


async def test_relay_publishes_pending_rows_and_marks_them(app, client, publisher, kafka_topic):
    [body] = await _upload(client)

    assert await _relay(app, publisher).relay_once() == (1, 0)

    [row] = await _outbox(app)
    assert row.status == OutboxStatus.PUBLISHED
    assert row.attempts == 1
    assert row.published_at is not None
    [msg] = await read_topic(kafka_topic, 1)
    event = TaskQueuedEvent.model_validate_json(msg.value)
    assert event.event_id == row.id
    assert str(event.task_id) == body["task"]["id"]
    assert msg.key == body["task"]["id"].encode()


async def test_empty_outbox_is_a_no_op(app, publisher):
    assert await _relay(app, publisher).relay_once() == (0, 0)


class _DownKafka:
    async def publish(self, topic, key, value):
        raise PublishError("kafka unavailable")


async def test_kafka_down_keeps_rows_pending_until_kafka_returns(app, client, publisher):
    await _upload(client, 2)

    assert await _relay(app, _DownKafka()).relay_once() == (0, 2)
    rows = await _outbox(app)
    assert {r.status for r in rows} == {OutboxStatus.PENDING}
    assert {r.attempts for r in rows} == {1}
    assert all("kafka unavailable" in r.last_error for r in rows)

    # "Kafka is back": the same rows go out, nothing was lost.
    assert await _relay(app, publisher).relay_once() == (2, 0)
    assert {r.status for r in await _outbox(app)} == {OutboxStatus.PUBLISHED}


async def test_crash_after_publish_before_commit_sends_a_duplicate(
    app, client, publisher, kafka_topic, monkeypatch: pytest.MonkeyPatch
):
    """Kafka got the message, the PUBLISHED mark did not commit -> sent again."""
    await _upload(client)

    async def crash(self, ids):
        raise RuntimeError("process died before COMMIT")

    with monkeypatch.context() as patch:
        patch.setattr(OutboxRepository, "mark_published", crash)
        with pytest.raises(RuntimeError, match="before COMMIT"):
            await _relay(app, publisher).relay_once()

    [row] = await _outbox(app)
    assert row.status == OutboxStatus.PENDING  # the UPDATE was rolled back

    assert await _relay(app, publisher).relay_once() == (1, 0)

    first, second = await read_topic(kafka_topic, 2)
    # Two copies of one message: same event_id. At-least-once, not exactly-once.
    assert first.value == second.value
    assert TaskQueuedEvent.model_validate_json(first.value).event_id == row.id


async def test_concurrent_relays_never_send_a_row_twice(app, client, publisher, kafka_topic):
    await _upload(client, 20)
    relays = [_relay(app, publisher, outbox_batch_size=3) for _ in range(3)]

    async def drain(relay: OutboxRelay) -> int:
        total = 0
        while (published := (await relay.relay_once())[0]) > 0:
            total += published
        return total

    counts = await asyncio.gather(*(drain(r) for r in relays))

    assert sum(counts) == 20
    ids = [
        TaskQueuedEvent.model_validate_json(m.value).event_id
        for m in await read_topic(kafka_topic, 20)
    ]
    assert len(set(ids)) == 20
