"""Messages exchanged over Kafka.

The wire format is JSON produced from these Pydantic models, so producer and
consumer share one schema definition and invalid messages fail validation
instead of crashing deeper inside the worker.
"""

import enum
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TaskQueuedEvent(BaseModel):
    """A task is ready to be processed. Topic: settings.kafka_topic."""

    model_config = ConfigDict(frozen=True)

    # Unique per message (not per task): lets consumers recognise a redelivery
    # of the same message vs. a new message about the same task.
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    task_id: uuid.UUID
    file_id: uuid.UUID
    attempt: int = 1

    @property
    def key(self) -> bytes:
        # Kafka message key -> partition. Keying by task_id keeps every event of
        # one task in one partition, hence in order.
        return str(self.task_id).encode()


class DeadLetterKind(enum.StrEnum):
    TASK_FAILED = "task_failed"  # a task ended FAILED (permanent error or attempts exhausted)
    INVALID_MESSAGE = "invalid_message"  # a message that is not a valid TaskQueuedEvent
    UNKNOWN_TASK = "unknown_task"  # a valid event naming a task that does not exist


class DeadLetter(BaseModel):
    """Something the pipeline gave up on. Topic: settings.kafka_dlq_topic.

    Self-contained on purpose: whoever reads the DLQ should understand what
    failed and why without digging through logs.
    """

    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    kind: DeadLetterKind
    error: str
    failed_at: datetime
    task_id: uuid.UUID | None = None
    file_id: uuid.UUID | None = None
    attempt: int | None = None
    # For messages rejected by the worker: where they came from and what they
    # contained (decoded leniently — they may not even be UTF-8).
    source: str | None = None  # "topic[partition]@offset"
    raw_value: str | None = None

    @property
    def key(self) -> bytes:
        # Dead letters of one task share a partition (and stay in order).
        return str(self.task_id or self.event_id).encode()


# Anything the outbox can carry.
OutboxMessage = TaskQueuedEvent | DeadLetter
