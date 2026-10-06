"""SQL for the `tasks` and `task_results` tables. Never commits; see FileRepository."""

import uuid

from sqlalchemy import select, update
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
        self, task_id: uuid.UUID, new: TaskStatus, **values: object
    ) -> Task | None:
        """Move a task to `new` if the state machine allows it from its current status.

        Returns None when the task is missing or in a status from which `new`
        cannot be entered (including "someone else got there first").
        """
        return await transition_status(
            self._session, Task, task_id, allowed_from=sources_of(new), new=new, **values
        )

    async def claim_next(self, **values: object) -> Task | None:
        """Atomically pick the oldest claimable task and move it to PROCESSING.

            UPDATE tasks SET status = 'PROCESSING', ...
            WHERE id = (SELECT id FROM tasks WHERE status IN ('PENDING')
                        ORDER BY created_at LIMIT 1
                        FOR UPDATE SKIP LOCKED)
            RETURNING *

        FOR UPDATE locks the chosen row until commit; SKIP LOCKED makes
        concurrent workers pass over rows another worker has locked instead of
        queueing behind it. Each worker gets a different task, nobody waits.
        """
        next_id = (
            select(Task.id)
            .where(Task.status.in_(sources_of(TaskStatus.PROCESSING)))
            .order_by(Task.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        stmt = (
            update(Task)
            .where(Task.id == next_id)
            .values(status=TaskStatus.PROCESSING, **values)
            .returning(Task)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    def add_result(self, result: TaskResult) -> None:
        self._session.add(result)

    async def get_result(self, task_id: uuid.UUID) -> TaskResult | None:
        return await self._session.get(TaskResult, task_id)
