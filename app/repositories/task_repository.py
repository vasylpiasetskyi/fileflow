"""SQL for the `tasks` and `task_results` tables. Never commits; see FileRepository."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Task, TaskResult, TaskStatus
from app.repositories.guarded_update import transition_status
from app.task_state_machine import sources_of


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, task: Task) -> None:
        self._session.add(task)

    async def get(self, task_id: uuid.UUID) -> Task | None:
        return await self._session.get(Task, task_id)

    async def transition(
        self,
        task_id: uuid.UUID,
        new: TaskStatus,
        *,
        expected_attempt: int | None = None,
        manual: bool = False,
        **values: object,
    ) -> Task | None:
        """Move a task to `new` if the state machine allows it from its current status.

        `expected_attempt` is a fencing token: when given, the change applies
        only if the task is still on that attempt, i.e. still owned by the caller who
        started it. Returns None when the task is missing, in a status from
        which `new` cannot be entered, or on another attempt. `manual` also
        allows operator transitions (MANUAL_TRANSITIONS).
        """
        where = (Task.attempt == expected_attempt,) if expected_attempt is not None else ()
        return await transition_status(
            self._session,
            Task,
            task_id,
            allowed_from=sources_of(new, manual=manual),
            new=new,
            where=where,
            **values,
        )

    async def lock_due_retries(self, limit: int) -> list[Task]:
        """Lock up to `limit` RETRY tasks whose next_attempt_at has passed.

            SELECT ... WHERE status = 'RETRY' AND next_attempt_at <= now()
            ORDER BY next_attempt_at LIMIT :limit FOR UPDATE SKIP LOCKED

        now() is the database clock, the same one that set next_attempt_at.
        """
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.RETRY, Task.next_attempt_at <= func.now())
            .order_by(Task.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self._session.scalars(stmt)).all())

    def add_result(self, result: TaskResult) -> None:
        self._session.add(result)

    async def get_result(self, task_id: uuid.UUID) -> TaskResult | None:
        return await self._session.get(TaskResult, task_id)
