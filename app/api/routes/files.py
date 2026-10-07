import uuid
from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, ConfigDict
from starlette.requests import ClientDisconnect

from app.api.deps import get_file_service, get_settings
from app.api.routes.tasks import TaskOut
from app.config import Settings
from app.models import FileStatus
from app.services.file_service import FileService, UnknownFileError, UploadTooLargeError
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


async def read_upload_chunks(upload: UploadFile, chunk_size: int) -> AsyncIterator[bytes]:
    # Starlette has already spooled the multipart body to a temp file (in
    # memory up to 1 MiB, then on disk). We read it back in bounded chunks and
    # never call `upload.read()` without a size.
    while chunk := await upload.read(chunk_size):
        yield chunk


async def _store(
    service: FileService, filename: str, content_type: str, chunks: AsyncIterable[bytes]
) -> UploadOut:
    try:
        result = await service.upload(filename=filename, content_type=content_type, chunks=chunks)
    except StorageError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "object storage unavailable"
        ) from exc
    except UploadTooLargeError as exc:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, str(exc)) from exc
    except ClientDisconnect as exc:
        # Nobody will read this response; it only keeps the log free of a 500.
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client disconnected") from exc
    return UploadOut(
        file=FileOut.model_validate(result.file), task=TaskOut.model_validate(result.task)
    )


@router.post("", response_model=UploadOut, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile,
    service: Annotated[FileService, Depends(get_file_service)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> UploadOut:
    """multipart/form-data upload (what an HTML form sends).

    Starlette parses the whole multipart body into a temporary file *before*
    this function runs: memory stays flat, but the file is written to the API's
    disk first and read back — twice the I/O, and disk space for the whole file.
    """
    if not file.filename:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "filename is required")
    return await _store(
        service,
        file.filename,
        file.content_type or "application/octet-stream",
        read_upload_chunks(file, settings.upload_read_chunk_bytes),
    )


@router.post("/stream", response_model=UploadOut, status_code=status.HTTP_201_CREATED)
async def upload_file_stream(
    request: Request,
    filename: Annotated[str, Query(min_length=1, max_length=255)],
    service: Annotated[FileService, Depends(get_file_service)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> UploadOut:
    """Raw-body upload, streamed end to end: socket -> hash -> MinIO.

        curl -X POST -T big.txt -H 'Content-Type: text/plain' \
             'localhost:8000/files/stream?filename=big.txt'

    Nothing is buffered beyond one S3 part. If MinIO is slow we read the socket
    more slowly, TCP's window fills up and the client is slowed down too:
    backpressure all the way to the sender.
    """
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > settings.max_upload_bytes:
        # Reject before touching the database or storage.
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE, f"upload exceeds {settings.max_upload_bytes} bytes"
        )
    content_type = request.headers.get("content-type") or "application/octet-stream"
    return await _store(service, filename, content_type, request.stream())


@router.get("/{file_id}", response_model=FileOut)
async def get_file(
    file_id: uuid.UUID,
    service: Annotated[FileService, Depends(get_file_service)],
) -> FileOut:
    try:
        return FileOut.model_validate(await service.get(file_id))
    except UnknownFileError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "file not found") from exc
