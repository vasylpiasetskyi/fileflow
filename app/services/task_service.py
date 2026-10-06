"""Task lifecycle. The worker (Phase 04) drives tasks through these methods.

Each method is one short transaction around a guarded UPDATE. When the guard
rejects a change we read the task once more, outside the decision, only to
report *why* (missing vs. wrong status) — the decision itself was already made
atomically by PostgreSQL.
"""

import uuid

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Task, TaskStatus
from app.repositories.task_repository import TaskRepository


class UnknownTaskError(LookupError):
    """No task with the given id."""


class TaskTransitionConflictError(RuntimeError):
    """The task's current status does not allow the requested transition."""

    def __init__(self, task_id: uuid.UUID, new: TaskStatus, current: TaskStatus) -> None:
        super().__init__(f"task {task_id}: cannot go {current} -> {new}")
        self.task_id = task_id
        self.new = new
        self.current = current


class TaskService:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker

    async def get(self, task_id: uuid.UUID) -> Task:
        async with self._sessionmaker() as session:
            task = await TaskRepository(session).get(task_id)
        if task is None:
            raise UnknownTaskError(task_id)
        return task

    async def start(self, task_id: uuid.UUID) -> Task:
        """Claim a task for processing: PENDING -> PROCESSING, attempt + 1."""
        return await self._transition(
            task_id,
            TaskStatus.PROCESSING,
            attempt=Task.attempt + 1,  # SQL expression, evaluated by PostgreSQL
            started_at=func.now(),
            finished_at=None,
        )

    async def complete(self, task_id: uuid.UUID) -> Task:
        return await self._transition(task_id, TaskStatus.COMPLETED, finished_at=func.now())

    async def fail(self, task_id: uuid.UUID, error: str) -> Task:
        return await self._transition(
            task_id, TaskStatus.FAILED, error=error, finished_at=func.now()
        )

    async def _transition(self, task_id: uuid.UUID, new: TaskStatus, **values: object) -> Task:
        async with self._sessionmaker.begin() as session:
            task = await TaskRepository(session).transition(task_id, new, **values)
        if task is not None:
            return task
        current = await self.get(task_id)  # raises UnknownTaskError if missing
        raise TaskTransitionConflictError(task_id, new, current.status)
