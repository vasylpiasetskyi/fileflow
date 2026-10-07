import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.api.routes.ui import _human_size, _time_column
from app.models import Task, TaskStatus
from app.repositories.task_repository import TaskOverview

NOW = datetime(2026, 10, 8, 12, 0, 0, tzinfo=UTC)


def _row(status: TaskStatus, processing_ms=None, **task_fields) -> TaskOverview:
    task = Task(id=uuid.uuid4(), status=status, attempt=1, max_attempts=5, **task_fields)
    return TaskOverview(task=task, filename="f", file_size=1, processing_ms=processing_ms)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (_row(TaskStatus.COMPLETED, processing_ms=1234), "1.2 s"),
        (_row(TaskStatus.COMPLETED, processing_ms=7), "7 ms"),
        (_row(TaskStatus.PROCESSING, started_at=NOW - timedelta(seconds=7)), "running 7 s"),
        (_row(TaskStatus.RETRY, next_attempt_at=NOW + timedelta(seconds=23)), "retry in 23 s"),
        (_row(TaskStatus.RETRY, next_attempt_at=NOW - timedelta(seconds=2)), "retry in 0 s"),
        (_row(TaskStatus.PENDING), "—"),
        (_row(TaskStatus.FAILED), "—"),
    ],
    ids=[
        "completed",
        "completed-fast",
        "processing",
        "retry",
        "retry-overdue",
        "pending",
        "failed",
    ],
)
def test_time_column(row, expected):
    assert _time_column(row, NOW) == expected


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (None, "—"),
        (512, "512 B"),
        (2048, "2.0 KiB"),
        (5 * 1024**2, "5.0 MiB"),
        (3 * 1024**3, "3.0 GiB"),
    ],
)
def test_human_size(size, expected):
    assert _human_size(size) == expected
