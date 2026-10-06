"""SQL for the `files` table.

A repository works inside a session it is given and never commits: the
caller (a service) decides where a transaction begins and ends.
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import File, FileStatus
from app.repositories.guarded_update import transition_status


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
        """Move a file from `expected` to `new`; None if it was not in `expected`."""
        return await transition_status(
            self._session, File, file_id, allowed_from={expected}, new=new, **values
        )
