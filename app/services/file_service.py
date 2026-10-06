"""File upload use case: metadata in PostgreSQL, bytes in object storage.

There is no transaction spanning PostgreSQL and S3, so the order of writes is
what keeps failures visible:

    tx1: INSERT files (UPLOADING)          -- committed before any byte is sent
         stream bytes -> S3 (multipart)
    tx2: UPLOADING -> UPLOADED + size/checksum
         INSERT tasks (PENDING)            -- same transaction: never one without the other

- S3 fails            -> row marked FAILED, no object (upload aborted).
- process dies midway -> row stays UPLOADING: an orphan you can find with SQL.
The reverse order (S3 first) would leave orphan objects nobody knows about.
"""

import hashlib
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import File, FileStatus, Task, TaskStatus
from app.repositories.file_repository import FileRepository
from app.repositories.task_repository import TaskRepository
from app.storage import Storage, StorageError


class UnknownFileError(LookupError):
    """No file with the given id."""


class InvalidFileTransitionError(RuntimeError):
    """The file was not in the status a transition expected."""


@dataclass(frozen=True)
class UploadResult:
    file: File
    task: Task


class StreamDigest:
    """Counts bytes and computes SHA-256 of a stream while passing it through."""

    def __init__(self) -> None:
        self.size = 0
        self._sha256 = hashlib.sha256()

    async def wrap(self, chunks: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
        async for chunk in chunks:
            self._sha256.update(chunk)
            self.size += len(chunk)
            yield chunk

    @property
    def sha256(self) -> str:
        return self._sha256.hexdigest()


class FileService:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession], storage: Storage) -> None:
        self._sessionmaker = sessionmaker
        self._storage = storage

    async def upload(
        self, filename: str, content_type: str, chunks: AsyncIterable[bytes]
    ) -> UploadResult:
        file_id = uuid.uuid4()
        storage_key = f"files/{file_id}"  # never derived from the user-supplied filename

        # tx1. `begin()` commits on exit, rolls back on exception.
        async with self._sessionmaker.begin() as session:
            FileRepository(session).add(
                File(
                    id=file_id,
                    filename=filename,
                    content_type=content_type,
                    storage_key=storage_key,
                    status=FileStatus.UPLOADING,
                )
            )

        digest = StreamDigest()
        try:
            await self._storage.upload_stream(storage_key, digest.wrap(chunks), content_type)
        except StorageError:
            async with self._sessionmaker.begin() as session:
                await self._finish_upload(session, file_id, FileStatus.FAILED)
            raise

        # tx2: the file becomes UPLOADED *and* gets its processing task atomically.
        # If anything here fails, both roll back and the file stays UPLOADING.
        async with self._sessionmaker.begin() as session:
            file = await self._finish_upload(
                session,
                file_id,
                FileStatus.UPLOADED,
                size=digest.size,
                checksum_sha256=digest.sha256,
            )
            task = Task(id=uuid.uuid4(), file_id=file_id, status=TaskStatus.PENDING, attempt=0)
            TaskRepository(session).add(task)
        return UploadResult(file=file, task=task)

    async def get(self, file_id: uuid.UUID) -> File:
        async with self._sessionmaker() as session:
            file = await FileRepository(session).get(file_id)
        if file is None:
            raise UnknownFileError(file_id)
        return file

    @staticmethod
    async def _finish_upload(
        session: AsyncSession, file_id: uuid.UUID, new: FileStatus, **values: object
    ) -> File:
        file = await FileRepository(session).transition(
            file_id, expected=FileStatus.UPLOADING, new=new, **values
        )
        if file is None:
            raise InvalidFileTransitionError(f"file {file_id} is not {FileStatus.UPLOADING}")
        return file
