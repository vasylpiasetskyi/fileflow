"""How the worker treats one Kafka message: commit, dead-letter, or rewind.

Fakes stand in for the consumer and the service: this is about offset
decisions, not about Kafka or PostgreSQL.
"""

import asyncio
import uuid

import pytest
from aiokafka import ConsumerRecord, TopicPartition

from app.config import Settings
from app.events import DeadLetter, DeadLetterKind, TaskQueuedEvent
from app.kafka import PublishError
from app.models import TaskStatus
from app.services.processing_service import Outcome, ProcessingResult
from app.services.task_service import UnknownTaskError
from app.worker.__main__ import handle

TP = TopicPartition("file-processing", 0)
SETTINGS = Settings(worker_redelivery_delay_s=0.01)


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list[dict] = []
        self.seeks: list[tuple[TopicPartition, int]] = []

    async def commit(self, offsets: dict) -> None:
        self.commits.append(offsets)

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self.seeks.append((tp, offset))


class FakeService:
    def __init__(self, outcome) -> None:
        self.outcome = outcome
        self.calls = 0

    async def process(self, event: TaskQueuedEvent):
        self.calls += 1
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[tuple[str, DeadLetter]] = []

    async def publish(self, topic: str, key: bytes, value: bytes):
        if self.fail:
            raise PublishError("dlq unreachable")
        self.sent.append((topic, DeadLetter.model_validate_json(value)))


PROCESSED = ProcessingResult(Outcome.PROCESSED, TaskStatus.COMPLETED, 1)


def _record(value: bytes, offset: int = 41) -> ConsumerRecord:
    return ConsumerRecord(
        topic=TP.topic,
        partition=TP.partition,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=value,
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(value),
        headers=(),
    )


def _event_bytes() -> bytes:
    return TaskQueuedEvent(task_id=uuid.uuid4(), file_id=uuid.uuid4()).model_dump_json().encode()


async def _handle(service, value: bytes, publisher=None) -> FakeConsumer:
    consumer = FakeConsumer()
    publisher = publisher or FakePublisher()
    await handle(consumer, service, publisher, _record(value), SETTINGS, asyncio.Event())
    return consumer


async def test_processed_message_commits_the_next_offset():
    consumer = await _handle(FakeService(PROCESSED), _event_bytes())

    assert consumer.commits == [{TP: 42}]  # offset 41 done -> next to read is 42
    assert consumer.seeks == []


async def test_invalid_message_is_dead_lettered_then_committed():
    service, publisher = FakeService(PROCESSED), FakePublisher()

    consumer = await _handle(service, b"garbage", publisher)

    assert service.calls == 0
    [(topic, letter)] = publisher.sent
    assert topic == SETTINGS.kafka_dlq_topic
    assert letter.kind is DeadLetterKind.INVALID_MESSAGE
    assert letter.raw_value == "garbage"
    assert letter.source == "file-processing[0]@41"
    assert consumer.commits == [{TP: 42}]


async def test_dlq_failure_rewinds_instead_of_losing_the_message():
    consumer = await _handle(FakeService(PROCESSED), b"garbage", FakePublisher(fail=True))

    assert consumer.commits == []  # not dead-lettered -> not done with it
    assert consumer.seeks == [(TP, 41)]


@pytest.mark.parametrize(
    "result",
    [
        ProcessingResult(Outcome.DUPLICATE, TaskStatus.COMPLETED, 1),
        ProcessingResult(Outcome.IN_PROGRESS, TaskStatus.PROCESSING, 1),
        ProcessingResult(Outcome.SCHEDULED, TaskStatus.RETRY, 1),
        ProcessingResult(Outcome.STALE, TaskStatus.PROCESSING, 2),
    ],
    ids=lambda r: r.outcome,
)
async def test_harmless_duplicates_are_committed_like_processed_messages(result):
    consumer = await _handle(FakeService(result), _event_bytes())

    assert consumer.commits == [{TP: 42}]
    assert consumer.seeks == []


async def test_event_for_an_unknown_task_is_dead_lettered():
    publisher = FakePublisher()

    consumer = await _handle(FakeService(UnknownTaskError()), _event_bytes(), publisher)

    [(_, letter)] = publisher.sent
    assert letter.kind is DeadLetterKind.UNKNOWN_TASK
    assert letter.task_id is not None
    assert consumer.commits == [{TP: 42}]


async def test_infrastructure_error_rewinds_instead_of_committing():
    consumer = await _handle(FakeService(ConnectionRefusedError("db down")), _event_bytes())

    assert consumer.commits == []  # outcome not stored -> must not commit
    assert consumer.seeks == [(TP, 41)]  # the same message will be fetched again
