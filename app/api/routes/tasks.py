import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict

from app.api.deps import get_task_service
from app.models import TaskStatus
from app.services.task_service import TaskService, UnknownTaskError

router = APIRouter(prefix="/tasks", tags=["tasks"])


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    file_id: uuid.UUID
    status: TaskStatus
    attempt: int
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@router.get("/{task_id}", response_model=TaskOut)
async def get_task(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
) -> TaskOut:
    try:
        return TaskOut.model_validate(await service.get(task_id))
    except UnknownTaskError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found") from exc
