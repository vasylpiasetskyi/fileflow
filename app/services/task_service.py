"""Task lifecycle: claim, complete, fail.

Each method is one short transaction around a guarded UPDATE. When the guard
rejects a change we read the task once more, outside the decision, only to
report *why* (missing vs. wrong status) — the decision itself was already made
atomically by PostgreSQL.
"""

import uuid

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Task, TaskResult, TaskStatus
from app.processing import FileStats
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


def _start_values(worker_id: str) -> dict[str, object]:
    """Column values for entering PROCESSING, shared by start() and claim_next()."""
    return {
        "attempt": Task.attempt + 1,  # SQL expression, evaluated by PostgreSQL
        "worker_id": worker_id,
        "started_at": func.now(),
        "finished_at": None,
    }


class TaskService:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker

    async def get(self, task_id: uuid.UUID) -> Task:
        async with self._sessionmaker() as session:
            task = await TaskRepository(session).get(task_id)
        if task is None:
            raise UnknownTaskError(task_id)
        return task

    async def get_result(self, task_id: uuid.UUID) -> TaskResult | None:
        async with self._sessionmaker() as session:
            return await TaskRepository(session).get_result(task_id)

    async def claim_next(self, worker_id: str) -> Task | None:
        """Claim the oldest PENDING task, if any (polling worker)."""
        async with self._sessionmaker.begin() as session:
            return await TaskRepository(session).claim_next(**_start_values(worker_id))

    async def start(self, task_id: uuid.UUID, worker_id: str) -> Task:
        """Claim a specific task: PENDING -> PROCESSING (consumer that is told the task id)."""
        return await self._transition(task_id, TaskStatus.PROCESSING, **_start_values(worker_id))

    async def complete(self, task_id: uuid.UUID, stats: FileStats, processing_ms: int) -> Task:
        """PROCESSING -> COMPLETED and store the result, in one transaction."""
        async with self._sessionmaker.begin() as session:
            repo = TaskRepository(session)
            task = await repo.transition(task_id, TaskStatus.COMPLETED, finished_at=func.now())
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

    async def fail(self, task_id: uuid.UUID, error: str) -> Task:
        return await self._transition(
            task_id, TaskStatus.FAILED, error=error, finished_at=func.now()
        )

    async def _transition(self, task_id: uuid.UUID, new: TaskStatus, **values: object) -> Task:
        async with self._sessionmaker.begin() as session:
            task = await TaskRepository(session).transition(task_id, new, **values)
        if task is None:
            raise await self._conflict(task_id, new)
        return task

    async def _conflict(self, task_id: uuid.UUID, new: TaskStatus) -> TaskTransitionConflictError:
        current = await self.get(task_id)  # raises UnknownTaskError if missing
        return TaskTransitionConflictError(task_id, new, current.status)
