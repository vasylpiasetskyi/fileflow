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
    max_attempts: int
    worker_id: str | None
    error: str | None
    created_at: datetime
    next_attempt_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


class TaskResultOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sha256: str
    size: int
    line_count: int
    word_count: int
    processing_ms: int


class TaskDetailOut(TaskOut):
    result: TaskResultOut | None


@router.get("/{task_id}", response_model=TaskDetailOut)
async def get_task(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
) -> TaskDetailOut:
    try:
        task = await service.get(task_id)
    except UnknownTaskError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found") from exc
    result = await service.get_result(task_id)
    return TaskDetailOut(
        **TaskOut.model_validate(task).model_dump(),
        result=TaskResultOut.model_validate(result) if result else None,
    )
