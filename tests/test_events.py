import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.events import DeadLetter, DeadLetterKind, TaskQueuedEvent


def test_event_round_trips_through_json():
    event = TaskQueuedEvent(task_id=uuid.uuid4(), file_id=uuid.uuid4())

    assert TaskQueuedEvent.model_validate_json(event.model_dump_json().encode()) == event
    assert event.attempt == 1


def test_each_event_gets_its_own_id():
    task_id, file_id = uuid.uuid4(), uuid.uuid4()
    first = TaskQueuedEvent(task_id=task_id, file_id=file_id)
    second = TaskQueuedEvent(task_id=task_id, file_id=file_id)

    assert first.event_id != second.event_id


def test_key_is_the_task_id_so_a_task_always_maps_to_one_partition():
    task_id = uuid.uuid4()
    event = TaskQueuedEvent(task_id=task_id, file_id=uuid.uuid4())

    assert event.key == str(task_id).encode()


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"{}", b'{"task_id": "nope", "file_id": "also-nope"}'],
)
def test_invalid_messages_fail_validation(raw):
    with pytest.raises(ValidationError):
        TaskQueuedEvent.model_validate_json(raw)


def test_dead_letter_round_trips_and_is_keyed_by_task():
    task_id = uuid.uuid4()
    letter = DeadLetter(
        kind=DeadLetterKind.TASK_FAILED,
        error="boom",
        failed_at=datetime.now(UTC),
        task_id=task_id,
        attempt=5,
    )

    assert DeadLetter.model_validate_json(letter.model_dump_json()) == letter
    assert letter.key == str(task_id).encode()


def test_dead_letter_without_task_is_keyed_by_its_own_id():
    letter = DeadLetter(
        kind=DeadLetterKind.INVALID_MESSAGE, error="not json", failed_at=datetime.now(UTC)
    )

    assert letter.key == str(letter.event_id).encode()
