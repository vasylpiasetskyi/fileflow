import uuid

import pytest
from pydantic import ValidationError

from app.events import TaskQueuedEvent


def test_event_round_trips_through_json():
    event = TaskQueuedEvent(task_id=uuid.uuid4(), file_id=uuid.uuid4())

    assert TaskQueuedEvent.model_validate_json(event.to_bytes()) == event
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
