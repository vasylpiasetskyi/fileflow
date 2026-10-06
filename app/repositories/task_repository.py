"""SQL for the `tasks` table. Never commits; see FileRepository."""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Task, TaskStatus
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
