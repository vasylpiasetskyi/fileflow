"""Server-rendered UI: one page, HTML fragments swapped in by HTMX.

There is no separate API for the UI: these routes call the same services as
the REST routes and render Jinja2 templates instead of JSON. HTMX requests a
fragment (hx-get / hx-post) and swaps it into the page.

HTMX does not swap 4xx/5xx responses by default, so UI routes report expected
errors (file too large, storage down) as a 200 fragment with a message. The
REST API keeps real status codes.
"""

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request, UploadFile
from fastapi.responses import HTMLResponse

from app.api.deps import (
    Probe,
    get_file_service,
    get_health_probes,
    get_settings,
    get_task_service,
    templates,
)
from app.api.routes.files import read_upload_chunks
from app.api.routes.health import check_all
from app.config import Settings
from app.models import TaskStatus
from app.repositories.task_repository import TaskOverview
from app.services.file_service import FileService, UnknownFileError, UploadTooLargeError
from app.services.task_service import TaskService, TaskTransitionConflictError, UnknownTaskError
from app.storage import StorageError

router = APIRouter(include_in_schema=False)

# Tells the task table (hx-trigger="tasks-changed from:body") to refresh now
# instead of waiting for its next poll.
TASKS_CHANGED = {"HX-Trigger": "tasks-changed"}


def _human_size(size: int | None) -> str:
    if size is None:
        return "—"
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return "—"


def _time_column(row: TaskOverview, now: datetime) -> str:
    """What the 'Time' column says, depending on where the task is."""
    task = row.task
    if row.processing_ms is not None:
        ms = row.processing_ms
        return f"{ms} ms" if ms < 1000 else f"{ms / 1000:.1f} s"
    if task.status is TaskStatus.PROCESSING and task.started_at:
        return f"running {(now - task.started_at).total_seconds():.0f} s"
    if task.status is TaskStatus.RETRY and task.next_attempt_at:
        return f"retry in {max(0, (task.next_attempt_at - now).total_seconds()):.0f} s"
    return "—"


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html", {"statuses": list(TaskStatus)})


@router.get("/ui/health", response_class=HTMLResponse)
async def health_badges(
    request: Request, probes: Annotated[dict[str, Probe], Depends(get_health_probes)]
) -> HTMLResponse:
    checks = await check_all(probes)
    return templates.TemplateResponse(request, "partials/health.html", {"checks": checks})


@router.get("/ui/tasks", response_class=HTMLResponse)
async def task_table(
    request: Request,
    tasks: Annotated[TaskService, Depends(get_task_service)],
    status: str = "",
) -> HTMLResponse:
    rows = await tasks.list_recent(limit=50, status=TaskStatus(status) if status else None)
    now = datetime.now(UTC)
    return templates.TemplateResponse(
        request,
        "partials/tasks.html",
        {
            "rows": [
                {"row": r, "time": _time_column(r, now), "size": _human_size(r.file_size)}
                for r in rows
            ]
        },
    )


async def _render_detail(
    request: Request,
    task_id: uuid.UUID,
    tasks: TaskService,
    files: FileService,
    message: str | None = None,
    headers: dict[str, str] | None = None,
) -> HTMLResponse:
    try:
        task = await tasks.get(task_id)
        file = await files.get(task.file_id)
    except (UnknownTaskError, UnknownFileError):
        return HTMLResponse('<p class="muted">Task not found.</p>')
    result = await tasks.get_result(task_id)
    return templates.TemplateResponse(
        request,
        "partials/task_detail.html",
        {
            "task": task,
            "file": file,
            "result": result,
            "size": _human_size(file.size),
            "message": message,
        },
        headers=headers,
    )


@router.get("/ui/tasks/{task_id}", response_class=HTMLResponse)
async def task_detail(
    request: Request,
    task_id: uuid.UUID,
    tasks: Annotated[TaskService, Depends(get_task_service)],
    files: Annotated[FileService, Depends(get_file_service)],
) -> HTMLResponse:
    return await _render_detail(request, task_id, tasks, files)


@router.post("/ui/tasks/{task_id}/redrive", response_class=HTMLResponse)
async def redrive(
    request: Request,
    task_id: uuid.UUID,
    tasks: Annotated[TaskService, Depends(get_task_service)],
    files: Annotated[FileService, Depends(get_file_service)],
) -> HTMLResponse:
    """The same operator action as `python -m app.dlq redrive`, as a button."""
    try:
        await tasks.redrive(task_id)
        message = "Redriven: back to PENDING with a new attempt budget."
    except TaskTransitionConflictError as exc:
        message = f"Not redriven: the task is {exc.current}, only FAILED tasks can be."
    except UnknownTaskError:
        message = "Not redriven: unknown task."
    return await _render_detail(request, task_id, tasks, files, message, TASKS_CHANGED)


@router.post("/ui/files", response_class=HTMLResponse)
async def upload(
    request: Request,
    file: UploadFile,
    files: Annotated[FileService, Depends(get_file_service)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> HTMLResponse:
    """Browser upload: an HTML form can only send multipart, so this is the
    spooled path; /files/stream needs a client that sends a raw body (curl, fetch)."""
    error = None
    result = None
    if not file.filename:
        error = "Choose a file first."
    else:
        try:
            result = await files.upload(
                filename=file.filename,
                content_type=file.content_type or "application/octet-stream",
                chunks=read_upload_chunks(file, settings.upload_read_chunk_bytes),
            )
        except UploadTooLargeError as exc:
            error = f"Too large: {exc}."
        except StorageError:
            error = "Object storage is unavailable, try again later."
    return templates.TemplateResponse(
        request,
        "partials/upload_result.html",
        {"result": result, "error": error},
        headers=TASKS_CHANGED if result else None,
    )
