"""Recovery: puts tasks back into the queue when their time has come.

Today: RETRY tasks whose next_attempt_at has passed. (Phase 11 adds tasks whose
worker lease expired — the same shape: find by time, back to PENDING, publish.)

One round = one transaction:

    BEGIN
      SELECT ... FOR UPDATE SKIP LOCKED   -- due RETRY tasks
      RETRY -> PENDING                    -- guarded, per task
      INSERT outbox_events                -- a new TaskQueuedEvent per task
    COMMIT

Requeueing and its event are atomic (the outbox again): a task can never be
PENDING without an event on its way, nor get an event while still RETRY.
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.events import TaskQueuedEvent
from app.models import TaskStatus
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.task_repository import TaskRepository

log = logging.getLogger(__name__)


class RecoveryService:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession], topic: str) -> None:
        self._sessionmaker = sessionmaker
        self._topic = topic

    async def requeue_due_retries(self, limit: int) -> int:
        """Requeue up to `limit` due RETRY tasks. Returns how many were requeued."""
        async with self._sessionmaker.begin() as session:
            tasks = TaskRepository(session)
            outbox = OutboxRepository(session)
            due = await tasks.lock_due_retries(limit)
            for task in due:
                requeued = await tasks.transition(task.id, TaskStatus.PENDING)
                if requeued is None:
                    # Impossible while we hold the row lock; fail loudly if not.
                    raise RuntimeError(f"task {task.id} changed under our lock")
                outbox.add(
                    self._topic,
                    # attempt = the attempt this event will trigger
                    TaskQueuedEvent(
                        task_id=task.id, file_id=task.file_id, attempt=task.attempt + 1
                    ),
                )
        for task in due:
            log.info("task %s: requeued for attempt %d", task.id, task.attempt + 1)
        return len(due)
