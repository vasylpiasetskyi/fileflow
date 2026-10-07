"""Task lifecycle: claim, complete, fail.

Each method is one short transaction around a guarded UPDATE. When the guard
rejects a change we read the task once more, outside the decision, only to
report *why* (missing, wrong status, other attempt) — the decision itself was
already made atomically by PostgreSQL.

Fencing: start() returns the task with its new `attempt` number. complete()
and fail() must present that number and succeed only if the task is still on
it. A worker that lost the task (e.g. recovered by another worker after its
lease expired) can therefore never overwrite the new owner's work.

Dead letters: fail() writes the DLQ message to the outbox in the same
transaction as FAILED — a failed task always gets its dead letter, and a dead
letter always describes a task that really failed.
"""

import uuid
from datetime import timedelta

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.events import DeadLetter, DeadLetterKind, TaskQueuedEvent
from app.models import Task, TaskResult, TaskStatus
from app.processing import FileStats
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.task_repository import TaskRepository


class UnknownTaskError(LookupError):
    """No task with the given id."""


class TaskTransitionConflictError(RuntimeError):
    """The task's current status (or attempt) does not allow the requested transition."""

    def __init__(self, task_id: uuid.UUID, new: TaskStatus, current: Task) -> None:
        super().__init__(
            f"task {task_id}: cannot go {current.status} (attempt {current.attempt}) -> {new}"
        )
        self.task_id = task_id
        self.new = new
        self.current = current.status
        self.current_attempt = current.attempt


class TaskService:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self._sessionmaker = sessionmaker
        self._settings = settings

    async def get(self, task_id: uuid.UUID) -> Task:
        async with self._sessionmaker() as session:
            task = await TaskRepository(session).get(task_id)
        if task is None:
            raise UnknownTaskError(task_id)
        return task

    async def get_result(self, task_id: uuid.UUID) -> TaskResult | None:
        async with self._sessionmaker() as session:
            return await TaskRepository(session).get_result(task_id)

    async def start(self, task_id: uuid.UUID, worker_id: str) -> Task:
        """Claim a task for processing: PENDING -> PROCESSING, attempt + 1."""
        return await self._transition(
            task_id,
            TaskStatus.PROCESSING,
            attempt=Task.attempt + 1,  # SQL expression, evaluated by PostgreSQL
            worker_id=worker_id,
            started_at=func.now(),
            finished_at=None,
            next_attempt_at=None,
        )

    async def complete(
        self, task_id: uuid.UUID, attempt: int, stats: FileStats, processing_ms: int
    ) -> Task:
        """PROCESSING -> COMPLETED and store the result, in one transaction.

        Only for the owner of `attempt`; the result row is inserted only if
        the guarded transition succeeded.
        """
        async with self._sessionmaker.begin() as session:
            repo = TaskRepository(session)
            task = await repo.transition(
                task_id,
                TaskStatus.COMPLETED,
                expected_attempt=attempt,
                finished_at=func.now(),
                error=None,  # earlier attempts may have failed; this one did not
            )
            if task is not None:
                repo.add_result(
                    TaskResult(
                        task_id=task_id,
                        sha256=stats.sha256,
                        size=stats.size,
                        line_count=stats.line_count,
                        word_count=stats.word_count,
                        processing_ms=processing_ms,
                    )
                )
        if task is None:
            raise await self._conflict(task_id, TaskStatus.COMPLETED)
        return task

    async def fail(self, task_id: uuid.UUID, attempt: int, error: str) -> Task:
        """PROCESSING -> FAILED plus its dead letter, atomically; owner of `attempt` only."""
        async with self._sessionmaker.begin() as session:
            task = await TaskRepository(session).transition(
                task_id,
                TaskStatus.FAILED,
                expected_attempt=attempt,
                error=error,
                finished_at=func.now(),
            )
            if task is not None:
                OutboxRepository(session).add(
                    self._settings.kafka_dlq_topic,
                    DeadLetter(
                        kind=DeadLetterKind.TASK_FAILED,
                        task_id=task.id,
                        file_id=task.file_id,
                        attempt=task.attempt,
                        error=error,
                        failed_at=task.finished_at,  # database clock, from RETURNING
                    ),
                )
        if task is None:
            raise await self._conflict(task_id, TaskStatus.FAILED)
        return task

    async def redrive(self, task_id: uuid.UUID) -> Task:
        """Operator action: FAILED -> PENDING with a fresh attempt budget, plus a new event.

        `attempt` is NOT reset — it is the fencing token and must only grow.
        Instead the budget is extended: max_attempts = attempt + task_max_attempts.
        """
        async with self._sessionmaker.begin() as session:
            task = await TaskRepository(session).transition(
                task_id,
                TaskStatus.PENDING,
                manual=True,
                max_attempts=Task.attempt + self._settings.task_max_attempts,
                next_attempt_at=None,
            )
            if task is not None:
                OutboxRepository(session).add(
                    self._settings.kafka_topic,
                    TaskQueuedEvent(
                        task_id=task.id, file_id=task.file_id, attempt=task.attempt + 1
                    ),
                )
        if task is None:
            raise await self._conflict(task_id, TaskStatus.PENDING)
        return task

    async def schedule_retry(
        self, task_id: uuid.UUID, attempt: int, error: str, delay_s: float
    ) -> Task:
        """PROCESSING -> RETRY, due in `delay_s` (database clock); owner of `attempt` only."""
        return await self._transition(
            task_id,
            TaskStatus.RETRY,
            expected_attempt=attempt,
            error=error,
            finished_at=func.now(),
            next_attempt_at=func.now() + timedelta(seconds=delay_s),
        )

    async def _transition(self, task_id: uuid.UUID, new: TaskStatus, **values: object) -> Task:
        async with self._sessionmaker.begin() as session:
            task = await TaskRepository(session).transition(task_id, new, **values)
        if task is None:
            raise await self._conflict(task_id, new)
        return task

    async def _conflict(self, task_id: uuid.UUID, new: TaskStatus) -> TaskTransitionConflictError:
        current = await self.get(task_id)  # raises UnknownTaskError if missing
        return TaskTransitionConflictError(task_id, new, current)
