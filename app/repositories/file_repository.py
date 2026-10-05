"""SQL for the `files` table.

A repository works inside a session it is given and never commits: the
caller (a service) decides where a transaction begins and ends.
"""

import uuid

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import File, FileStatus


class FileRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, file: File) -> None:
        self._session.add(file)

    async def get(self, file_id: uuid.UUID) -> File | None:
        return await self._session.get(File, file_id)

    async def transition(
        self,
        file_id: uuid.UUID,
        *,
        expected: FileStatus,
        new: FileStatus,
        **values: object,
    ) -> File | None:
        """Move a file from `expected` to `new` status in one atomic UPDATE.

        The `WHERE status = expected` guard makes the transition safe against
        concurrent writers: if someone else already changed the status, zero
        rows match and None is returned instead of silently overwriting.
        """
        stmt = (
            update(File)
            .where(File.id == file_id, File.status == expected)
            .values(status=new, **values)
            .returning(File)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()
