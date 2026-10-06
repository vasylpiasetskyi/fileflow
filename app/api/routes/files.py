import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from pydantic import BaseModel, ConfigDict

from app.api.deps import get_file_service, get_settings
from app.api.routes.tasks import TaskOut
from app.config import Settings
from app.kafka import PublishError
from app.models import FileStatus
from app.services.file_service import FileService, UnknownFileError
from app.storage import StorageError

router = APIRouter(prefix="/files", tags=["files"])


class FileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    filename: str
    content_type: str
    size: int | None
    checksum_sha256: str | None
    storage_key: str
    status: FileStatus
    created_at: datetime


class UploadOut(BaseModel):
    file: FileOut
    task: TaskOut


async def _read_chunks(upload: UploadFile, chunk_size: int) -> AsyncIterator[bytes]:
    # Starlette has already spooled the multipart body to a temp file (in
    # memory up to 1 MiB, then on disk). We read it back in bounded chunks and
    # never call `upload.read()` without a size.
    while chunk := await upload.read(chunk_size):
        yield chunk


@router.post("", response_model=UploadOut, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile,
    service: Annotated[FileService, Depends(get_file_service)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> UploadOut:
    if not file.filename:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "filename is required")
    try:
        result = await service.upload(
            filename=file.filename,
            content_type=file.content_type or "application/octet-stream",
            chunks=_read_chunks(file, settings.upload_read_chunk_bytes),
        )
    except StorageError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "object storage unavailable"
        ) from exc
    except PublishError as exc:
        # The file and its task ARE committed, but no worker will hear about it.
        # The client gets an error for an upload that half-succeeded: the
        # dual-write problem made visible (fixed by the outbox in Phase 06).
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "file stored but could not be queued"
        ) from exc
    return UploadOut(
        file=FileOut.model_validate(result.file), task=TaskOut.model_validate(result.task)
    )


@router.get("/{file_id}", response_model=FileOut)
async def get_file(
    file_id: uuid.UUID,
    service: Annotated[FileService, Depends(get_file_service)],
) -> FileOut:
    try:
        return FileOut.model_validate(await service.get(file_id))
    except UnknownFileError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "file not found") from exc
