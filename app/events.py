"""Messages exchanged over Kafka.

The wire format is JSON produced from these Pydantic models, so producer and
consumer share one schema definition and invalid messages fail validation
instead of crashing deeper inside the worker.
"""

import uuid

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
